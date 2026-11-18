import {lock_drillsApi} from '../api/lock_drills.js';
export const lock_drillsStore={items:[],async refresh(){this.items=await lock_drillsApi.list();return this.items;}};
