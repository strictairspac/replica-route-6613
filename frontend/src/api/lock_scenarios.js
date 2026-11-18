import {api} from './client.js';
export const lock_scenariosApi={list:()=>api('/lock_scenarios'),create:data=>api('/lock_scenarios','POST',data)};
