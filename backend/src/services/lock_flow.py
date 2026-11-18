"""受控写锁争用演练：等待环检测、裁决复核、幂等重试与提交后读选路。

模型只在内存中模拟行锁管理器，不连接真实数据库锁，也不实现完整事务隔离级别。
acquire 视为“已对该行完成写入”，commit 才把写入推进到复制位点；rollback 丢弃写入。
"""
import copy, hashlib, json

EVENT_KINDS = ('begin', 'acquire', 'check', 'decide', 'retry_rollback', 'commit', 'rollback', 'replicate', 'node_down', 'node_up', 'read')
POLICIES = ('min_writes', 'max_blocking', 'youngest')
POLICY_LABELS = {'min_writes': '保全已做写入', 'max_blocking': '优先解开积压', 'youngest': '回滚最新事务'}
MAX_EVENTS = 120
MAX_TX = 24
MAX_RESOURCES = 16
MAX_AT_MS = 86400000


def _check_event_shape(events):
    if not 1 <= len(events) <= MAX_EVENTS:
        raise ValueError(f'需要1–{MAX_EVENTS}个时序事件')
    last = -1
    for event in events:
        at = event.get('at_ms')
        if not isinstance(at, int) or at < 0 or at > MAX_AT_MS:
            raise ValueError('事件时间必须是0–86400000的整数')
        if at < last:
            raise ValueError('事件必须按at_ms非递减排列')
        last = at
        kind = event.get('kind')
        if kind not in EVENT_KINDS:
            raise ValueError('未知事件类型：' + str(kind))
        if kind in ('begin', 'acquire', 'commit', 'rollback'):
            if not event.get('tx'):
                raise ValueError(kind + '事件需要tx')
        if kind == 'acquire' and not event.get('resource'):
            raise ValueError('acquire事件需要resource')
        if kind == 'retry_rollback':
            rid = event.get('rollback_id')
            if not isinstance(rid, int) or rid < 1:
                raise ValueError('retry_rollback事件需要正整数rollback_id')
        if kind in ('replicate', 'node_down', 'node_up') and not event.get('node'):
            raise ValueError('节点事件需要node')
        if kind == 'replicate' and (event.get('position') is None or event.get('delay_ms') is None):
            raise ValueError('replicate需要position和delay_ms')


def _canonical(cycle):
    smallest = cycle.index(min(cycle))
    return tuple(cycle[smallest:] + cycle[:smallest])


def _wait_edges(txs, locks):
    edges = {}
    for name, tx in txs.items():
        if tx['state'] == 'waiting' and tx['waiting_for']:
            holder = locks[tx['waiting_for']]['holder']
            if holder and holder != name:
                edges[name] = holder
    return edges


def _find_cycles(txs, locks):
    edges = _wait_edges(txs, locks)
    found, seen = [], set()
    for start in txs:
        if start not in edges:
            continue
        path, position, cursor = [], {}, start
        while cursor in edges and cursor not in position:
            position[cursor] = len(path)
            path.append(cursor)
            cursor = edges[cursor]
        if cursor in position:
            cycle = _canonical(tuple(path[position[cursor]:]))
            if cycle not in seen:
                seen.add(cycle)
                found.append(list(cycle))
    return sorted(found, key=lambda c: (len(c), tuple(c)))


def _cycle_intact(cycle, txs, locks):
    edges = _wait_edges(txs, locks)
    return all(edges.get(cycle[i]) == cycle[(i + 1) % len(cycle)] for i in range(len(cycle)))


def _pick_victim(candidates, txs, locks, policy):
    def blocked(name):
        return sum(1 for tx in txs.values() if tx['state'] == 'waiting' and locks[tx['waiting_for']]['holder'] == name)
    if policy == 'youngest':
        return min(candidates, key=lambda n: (-txs[n]['seq'], len(txs[n]['writes']), n))
    if policy == 'max_blocking':
        return min(candidates, key=lambda n: (-blocked(n), len(txs[n]['writes']), -txs[n]['seq'], n))
    return min(candidates, key=lambda n: (len(txs[n]['writes']), -txs[n]['seq'], n))


def _route_read(nodes, primary_name, lineage, session_state, at_ms, max_delay_ms, sticky_ms):
    required = session_state.get('required', 0)
    last_write = session_state.get('last_write_ms')
    sticky = last_write is not None and at_ms - last_write < sticky_ms
    primary = nodes[primary_name]
    replicas = [n for n in nodes.values() if n['role'] == 'replica']
    checks = [{'name': n['name'], 'healthy': n['status'] == 'healthy', 'same_lineage': n['lineage'] == lineage,
               'position_sufficient': n['position'] >= required, 'delay_within_limit': n['delay_ms'] <= max_delay_ms}
              for n in replicas]
    eligible = [n for n in replicas if n['status'] == 'healthy' and n['lineage'] == lineage
                and n['position'] >= required and n['delay_ms'] <= max_delay_ms]
    candidates = sorted(eligible, key=lambda n: (n['delay_ms'], -n['position'], n['name']))
    if sticky:
        if primary['status'] != 'healthy':
            return None, None, '粘滞期主库不在线，拒绝不确定读取', checks
        if primary['lineage'] != lineage:
            return None, None, '粘滞期主库谱系不匹配，拒绝不确定读取', checks
        if primary['position'] < required:
            return None, None, '粘滞期主库位点落后于会话写位点，拒绝不确定读取', checks
        return primary['name'], primary, '写后读粘滞：保持主库', checks
    if candidates:
        return candidates[0]['name'], candidates[0], '副本位点满足会话且延迟达标', checks
    if primary['status'] == 'healthy' and primary['lineage'] == lineage and primary['position'] >= required:
        return primary['name'], primary, '无安全达标副本，回退主库', checks
    if primary['status'] != 'healthy':
        return None, None, '主库不在线且无安全达标副本，拒绝读取', checks
    return None, None, '主库位点落后且无安全达标副本，拒绝读取', checks


def _run(nodes_input, events, victim_policy, max_delay_ms, sticky_ms):
    if victim_policy not in POLICIES:
        raise ValueError('未知裁决策略：' + str(victim_policy))
    if not 1 <= len(nodes_input) <= 12:
        raise ValueError('需要1–12个节点')
    if max_delay_ms < 0 or sticky_ms < 0:
        raise ValueError('延迟门槛和粘滞时间不得为负')
    if len({x['name'] for x in nodes_input}) != len(nodes_input):
        raise ValueError('节点名必须唯一')
    primaries = [x for x in nodes_input if x['role'] == 'primary']
    if len(primaries) != 1:
        raise ValueError('必须且只能配置一个主节点')
    _check_event_shape(events)
    nodes = {x['name']: copy.deepcopy(x) for x in nodes_input}
    primary_name = primaries[0]['name']
    primary = nodes[primary_name]
    lineage = primary['lineage']
    base_position = primary['position']
    if any(n['role'] == 'replica' and n['lineage'] == lineage and n['position'] > base_position for n in nodes.values()):
        raise ValueError('同日志谱系副本位点不得高于主库初始位点')
    log_position = base_position
    txs = {}
    locks = {}
    sessions = {}
    detections = []
    receipts = {}
    next_rollback_id = 1
    trace = []
    waiting_events = 0
    peak_waiting = 0
    last_waiting_at = None
    writes_committed = 0
    writes_discarded = 0
    deadlock_rollbacks = 0
    stale_decisions = 0
    retry_noops = 0

    def waiting_count():
        return sum(1 for tx in txs.values() if tx['state'] == 'waiting')

    def tx_row(name):
        tx = txs.get(name)
        return {} if not tx else {'tx_state': tx['state'], 'waits_for': tx['waiting_for'],
                                  'writes_held': len(tx['writes']), 'committed_position': tx['committed_position'],
                                  'rollback_id': tx['rollback_id']}

    def release_locks(tx):
        released, granted = [], []
        for resource in list(tx['held']):
            lock = locks[resource]
            lock['holder'] = None
            released.append(resource)
            while lock['queue']:
                candidate = lock['queue'].pop(0)
                waiter = txs.get(candidate)
                if waiter and waiter['state'] == 'waiting' and waiter['waiting_for'] == resource:
                    lock['holder'] = candidate
                    waiter['state'] = 'active'
                    waiter['waiting_for'] = None
                    waiter['held'].append(resource)
                    waiter['writes'].append(resource)
                    granted.append({'tx': candidate, 'resource': resource})
                    break
        tx['held'] = []
        return released, granted

    def finish_commit(tx, at_ms):
        nonlocal log_position, writes_committed
        writes = list(tx['writes'])
        released, granted = release_locks(tx)
        log_position += len(writes)
        primary['position'] = log_position
        writes_committed += len(writes)
        tx['state'] = 'committed'
        tx['commit_pending'] = False
        tx['committed_position'] = log_position if writes else None
        if writes:
            sessions[tx['name']] = {'required': log_position, 'last_write_ms': at_ms, 'lineage': lineage}
        return {'tx': tx['name'], 'writes': writes, 'released': released, 'granted_to': granted,
                'position': tx['committed_position']}

    def drain_pending_commits(at_ms, row):
        done = []
        while True:
            ready = sorted((t for t in txs.values() if t['state'] == 'active' and t['commit_pending']),
                           key=lambda t: t['seq'])
            if not ready:
                return done
            done.append(finish_commit(ready[0], at_ms))
            row['released'] = row['released'] + done[-1]['released']
            row['granted_to'] = row['granted_to'] + done[-1]['granted_to']

    def rollback_tx(tx, at_ms, reason, detection_id=None, forced_id=None, origin='app'):
        nonlocal writes_discarded, next_rollback_id
        if tx['waiting_for'] and tx['waiting_for'] in locks:
            queue = locks[tx['waiting_for']]['queue']
            if tx['name'] in queue:
                queue.remove(tx['name'])
        released, granted = release_locks(tx)
        discarded = len(tx['writes'])
        writes_discarded += discarded
        tx['writes'] = []
        tx['waiting_for'] = None
        tx['state'] = 'rolled_back'
        tx['commit_pending'] = False
        rid = forced_id if forced_id is not None else next_rollback_id
        receipt = {'id': rid, 'tx': tx['name'], 'at_ms': at_ms, 'reason': reason, 'origin': origin,
                   'detection_id': detection_id, 'released': released, 'granted_to': granted,
                   'writes_discarded': discarded, 'replication_position': log_position}
        tx['rollback_id'] = rid
        next_rollback_id = max(next_rollback_id, rid + 1)
        receipts[rid] = receipt
        return receipt

    for index, event in enumerate(events):
        at, kind = event['at_ms'], event['kind']
        name = event.get('tx')
        row = {'index': index + 1, 'at_ms': at, 'kind': kind, 'tx': name, 'resource': event.get('resource'),
               'decision': 'applied', 'reason': '', 'waits_for': None, 'released': [], 'granted_to': [],
               'cycle': [], 'victim': None, 'stale': False, 'duplicate': False, 'rollback_id': None,
               'replication_before': log_position, 'replication_after': log_position,
               'log_position': log_position, 'route': None}
        if kind == 'begin':
            if name in txs and txs[name]['state'] in ('active', 'waiting'):
                raise ValueError('事务已在进行中：' + name)
            if len(txs) >= MAX_TX:
                raise ValueError(f'最多{MAX_TX}个事务')
            seq = max((t['seq'] for t in txs.values()), default=0) + 1
            txs[name] = {'name': name, 'seq': seq, 'state': 'active', 'started_at': at, 'held': [],
                         'waiting_for': None, 'writes': [], 'committed_position': None, 'rollback_id': None,
                         'commit_pending': False}
            row.update(decision='begun', reason='事务开始', **tx_row(name))
        elif kind == 'acquire':
            tx = txs.get(name)
            if not tx:
                raise ValueError('事务未开始：' + str(name))
            resource = event['resource']
            if len(resource) > 40:
                raise ValueError('锁资源名最长40字符')
            if tx['state'] != 'active':
                raise ValueError('事务当前不能申请锁：' + name)
            if tx['commit_pending']:
                raise ValueError('事务已请求提交，不能继续申请锁：' + name)
            lock = locks.setdefault(resource, {'holder': None, 'queue': []})
            if len(locks) > MAX_RESOURCES:
                raise ValueError(f'最多{MAX_RESOURCES}个锁资源')
            if lock['holder'] == name or resource in tx['held']:
                row.update(decision='granted', reason='事务已持有该行锁（重复申请不再计写入）')
            elif lock['holder'] is None:
                lock['holder'] = name
                tx['held'].append(resource)
                tx['writes'].append(resource)
                row.update(decision='granted', reason='行锁获取成功，等价于已完成该行写入')
            else:
                holder = lock['holder']
                lock['queue'].append(name)
                tx['waiting_for'] = resource
                tx['state'] = 'waiting'
                row.update(decision='waiting', reason=f'等待 {holder} 释放 {resource}', waits_for=resource)
            row.update(**tx_row(name))
        elif kind == 'check':
            cycles = _find_cycles(txs, locks)
            if cycles:
                detection = {'id': len(detections) + 1, 'at_ms': at, 'cycles': cycles}
                detections.append(detection)
                row.update(decision='detected', cycle=cycles, reason=f'检测到 {len(cycles)} 个等待环，等待裁决复核')
            else:
                row.update(decision='clear', reason='检查完成：当前没有等待环')
        elif kind == 'decide':
            pending = detections.pop(0) if detections else None
            intact, broken = [], []
            if pending:
                for cycle in pending['cycles']:
                    (intact if _cycle_intact(cycle, txs, locks) else broken).append(cycle)
            if pending and not intact:
                stale_decisions += 1
                new_cycles = _find_cycles(txs, locks)
                row.update(decision='skipped', stale=True, cycle=broken, reason='等待环已解开，不按旧关系回滚；本次裁决不释放任何锁',
                           log_position=log_position)
                if new_cycles:
                    row['reason'] += f'（当前另有 {len(new_cycles)} 个新等待环，需重新检查）'
            elif intact or not pending:
                cycles = intact or _find_cycles(txs, locks)
                if not cycles:
                    row.update(decision='clear', reason='裁决时复核：等待环已不存在，不回滚任何事务')
                else:
                    candidates = sorted({n for cycle in cycles for n in cycle})
                    victim_name = _pick_victim(candidates, txs, locks, victim_policy)
                    victim = txs[victim_name]
                    policy_note = {'min_writes': '牺牲已做写入最少的事务', 'max_blocking': '牺牲解除等待最多的事务',
                                   'youngest': '牺牲最后开始的事务'}[victim_policy]
                    receipt = rollback_tx(victim, at, f'死锁裁决（{policy_note}）', pending['id'] if pending else None, origin='deadlock')
                    deadlock_rollbacks += 1
                    row.update(decision='rolled_back', cycle=cycles, victim=victim_name,
                               released=receipt['released'], granted_to=receipt['granted_to'],
                               reason=f'复核确认等待环仍在，回滚 {victim_name}（{policy_note}），其余事务继续',
                               **tx_row(victim_name))
                    row['rollback_id'] = receipt['id']
        elif kind == 'retry_rollback':
            rid = event['rollback_id']
            receipt = receipts.get(rid)
            if receipt:
                retry_noops += 1
                row.update(decision='duplicate', duplicate=True, rollback_id=rid, tx=receipt['tx'],
                           reason=f'回滚结果 #{rid} 已生效：重试不重复放锁，复制位点保持不变',
                           replication_before=log_position, replication_after=log_position,
                           rollback_position=receipt['replication_position'])
            else:
                target = txs.get(name) if name else None
                if target and target['state'] in ('active', 'waiting'):
                    receipt = rollback_tx(target, at, f'重试执行未完成的裁决回滚 #{rid}', forced_id=rid)
                    row.update(decision='applied_on_retry', tx=target['name'],
                               released=receipt['released'], granted_to=receipt['granted_to'],
                               reason='裁决回滚此前未执行，本次补做并记录回执；复制位点不推进', **tx_row(target['name']))
                    row['rollback_id'] = rid
                else:
                    row.update(decision='ignored',
                               reason=f'未找到回滚回执 #{rid}，且无待回滚事务；重试被安全忽略',
                               **(tx_row(name) if name else {}))
                    row['rollback_id'] = rid
        elif kind == 'commit':
            tx = txs.get(name)
            if not tx:
                raise ValueError('事务未开始：' + str(name))
            if tx['state'] == 'rolled_back':
                row.update(decision='skipped', reason='事务已回滚，提交被跳过', **tx_row(name))
            elif tx['state'] == 'waiting':
                tx['commit_pending'] = True
                row.update(decision='deferred', reason=f'仍在等待 {tx["waiting_for"]}，提交挂起，获锁后自动完成', **tx_row(name))
            elif tx['state'] == 'committed':
                row.update(decision='skipped', reason='事务已提交（重复提交被忽略）', **tx_row(name))
            else:
                done = finish_commit(tx, at)
                own_position = log_position
                drained = drain_pending_commits(at, row)
                reason = f'提交 {len(done["writes"])} 笔写入并放锁，日志位点推进到 {own_position}'
                if drained:
                    row['drained_commits'] = drained
                    reason += '；获锁后挂起提交随之完成：' + '、'.join(d['tx'] for d in drained) + f'（位点到 {log_position}）'
                row.update(decision='committed', released=done['released'] + row['released'],
                           granted_to=done['granted_to'] + row['granted_to'],
                           reason=reason, **tx_row(name))
        elif kind == 'rollback':
            tx = txs.get(name)
            if not tx:
                raise ValueError('事务未开始：' + str(name))
            if tx['state'] in ('rolled_back', 'committed'):
                row.update(decision='skipped', reason='事务已结束，无需回滚', **tx_row(name))
            else:
                receipt = rollback_tx(tx, at, '应用主动回滚')
                row.update(decision='rolled_back', released=receipt['released'], granted_to=receipt['granted_to'],
                           reason=f'应用主动回滚，丢弃 {receipt["writes_discarded"]} 笔未提交写入', **tx_row(name))
        elif kind == 'replicate':
            target = nodes.get(event['node'])
            if not target:
                raise ValueError('事件引用未知节点：' + str(event['node']))
            if target['role'] != 'replica':
                raise ValueError('replicate仅允许副本节点')
            if target['status'] != 'healthy':
                raise ValueError('停机副本不能应用日志，请先node_up')
            if target['lineage'] != lineage:
                raise ValueError('日志谱系不匹配，不得复制当前主库日志')
            position, delay = event['position'], event['delay_ms']
            if delay < 0 or not target['position'] <= position <= log_position:
                raise ValueError('复制位点必须单调且不超过已提交主库位点')
            target['position'] = position
            target['delay_ms'] = delay
            row.update(decision='applied', reason='副本应用已提交日志')
        elif kind in ('node_down', 'node_up'):
            target = nodes.get(event['node'])
            if not target:
                raise ValueError('事件引用未知节点：' + str(event['node']))
            target['status'] = 'down' if kind == 'node_down' else 'healthy'
            row.update(decision='applied', reason='节点下线' if kind == 'node_down' else '节点恢复')
        elif kind == 'read':
            session = event.get('session') or 'default'
            state = sessions.get(session, {'required': 0, 'lineage': lineage, 'last_write_ms': None})
            required = state.get('required', 0)
            chosen, node, reason, checks = _route_read(nodes, primary_name, lineage, state, at, max_delay_ms, sticky_ms)
            row.update(tx=session, decision='routed' if chosen else 'rejected', reason=reason,
                       waits_for=None, route={'node': chosen, 'required_position': required,
                                              'observed_position': node['position'] if node else None,
                                              'safe': bool(node and node['position'] >= required and node['lineage'] == lineage),
                                              'checks': checks})
            if not chosen:
                row['decision'] = 'rejected'
        drained = drain_pending_commits(at, row)
        if drained:
            row['drained_commits'] = drained
            row['reason'] = (row['reason'] + '；' if row['reason'] else '') + '获锁后挂起提交随之完成：' + '、'.join(d['tx'] for d in drained)
        waiting = waiting_count()
        if waiting:
            waiting_events += 1
            last_waiting_at = at
            peak_waiting = max(peak_waiting, waiting)
        row['waiting_total'] = waiting
        row['log_position'] = log_position
        row['replication_after'] = log_position
        trace.append(row)

    transactions = []
    for tx in txs.values():
        transactions.append({'name': tx['name'], 'state': tx['state'], 'started_at': tx['started_at'],
                             'waiting_for': tx['waiting_for'], 'writes': list(tx['writes']),
                             'writes_count': len(tx['writes']), 'committed_position': tx['committed_position'],
                             'rollback_id': tx['rollback_id']})
    lock_rows = [{'resource': r, 'holder': l['holder'], 'queue': list(l['queue'])} for r, l in sorted(locks.items())]
    receipts_out = [receipts[k] for k in sorted(receipts)]
    summary = {
        'transactions': len(txs), 'committed_transactions': sum(1 for t in txs.values() if t['state'] == 'committed'),
        'rolled_back_transactions': sum(1 for t in txs.values() if t['state'] == 'rolled_back'),
        'final_waiting': waiting_count(), 'writes_committed': writes_committed, 'writes_discarded': writes_discarded,
        'deadlocks_detected': sum(1 for row in trace if row['decision'] == 'detected'),
        'deadlock_rollbacks': deadlock_rollbacks, 'stale_decisions': stale_decisions, 'retry_noops': retry_noops,
        'reads': sum(1 for row in trace if row['kind'] == 'read'),
        'read_rejections': sum(1 for row in trace if row['kind'] == 'read' and row['decision'] == 'rejected'),
        'peak_waiting': peak_waiting, 'waiting_events': waiting_events, 'last_waiting_at_ms': last_waiting_at,
        'log_position': log_position,
    }
    result = {
        'mode': '受控写锁争用模型 / 非真实数据库锁管理器',
        'lineage': lineage,
        'policy': {'victim_policy': victim_policy, 'victim_policy_label': POLICY_LABELS[victim_policy],
                   'max_delay_ms': max_delay_ms, 'sticky_ms': sticky_ms,
                   'victim_tie_break': 'min_writes=写入少→开始晚；max_blocking=阻塞多→写入少→开始晚；youngest=开始最晚',
                   'note': 'acquire 记为该行写入，commit 才推进复制位点；rollback 丢弃未提交写入'},
        'trace': trace, 'transactions': transactions, 'locks': lock_rows, 'sessions': sessions,
        'final_nodes': [nodes[k] for k in sorted(nodes)],
        'rollback_receipts': receipts_out, 'pending_detections': detections, 'summary': summary,
    }
    return result


def _cost_view(result):
    summary = result['summary']
    deadlock_receipts = [r for r in result['rollback_receipts'] if r.get('origin') == 'deadlock']
    return {
        'policy': result['policy']['victim_policy'], 'label': result['policy']['victim_policy_label'],
        'victims': [r['tx'] for r in deadlock_receipts],
        'all_rollback_receipts': [r['id'] for r in result['rollback_receipts']],
        'writes_committed': summary['writes_committed'],
        'writes_discarded': summary['writes_discarded'], 'deadlock_rollbacks': summary['deadlock_rollbacks'],
        'stale_decisions': summary['stale_decisions'], 'retry_noops': summary['retry_noops'],
        'freed_at_decisions': sum(len(row['granted_to']) for row in result['trace'] if row['kind'] == 'decide'),
        'peak_waiting': summary['peak_waiting'], 'waiting_events': summary['waiting_events'],
        'last_waiting_at_ms': summary['last_waiting_at_ms'], 'final_waiting': summary['final_waiting'],
        'committed_tx': [t['name'] for t in result['transactions'] if t['state'] == 'committed'],
        'rolled_back_tx': [t['name'] for t in result['transactions'] if t['state'] == 'rolled_back'],
        'rollback_receipts': [r['id'] for r in result['rollback_receipts']],
    }


def simulate_lock_drill(nodes_input, events, victim_policy='min_writes', max_delay_ms=100, sticky_ms=500):
    result = _run(nodes_input, events, victim_policy, max_delay_ms, sticky_ms)
    comparison = [_cost_view(_run(nodes_input, events, policy, max_delay_ms, sticky_ms)) for policy in POLICIES]
    result['comparison'] = comparison
    result['tradeoff'] = {
        'chosen_policy': victim_policy,
        'same_scenario': True,
        'default_rule': '默认保全已做写入：先回滚写入最少的事务，积压事务多等一段但保留更多成功写入。',
        'alternative_rule': '同一场景切换 max_blocking：立即解除最多等待，但可能多丢弃写入。',
        'metric_note': 'last_waiting_at_ms 越小表示积压越早清空；writes_discarded 是回滚丢弃的已做写入。',
    }
    result['checksum'] = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return result
