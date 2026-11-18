import {api,downloadLock} from './client.js';
export const lock_drillsApi={list:()=>api('/lock_drills'),create:data=>api('/lock_drills','POST',data),export:downloadLock};
