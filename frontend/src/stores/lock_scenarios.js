import {lock_scenariosApi} from '../api/lock_scenarios.js';
export const lock_scenariosStore={items:[],async refresh(){this.items=await lock_scenariosApi.list();return this.items;}};
