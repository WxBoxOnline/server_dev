var Pool = require('pg-pool')
const funcs = require("./funcs")

const dbAdmin = new Pool({
  user: 'root',
  host: '127.0.0.1',
  database: 'wxbox',
  password: 'SailView2627!',
  port: 5432,
  max: 10, 
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 120000, 
  maxUses: 100,
})

async function getAdminDb() {
  return dbAdmin
}

const uvDbPool = new Pool({
  user: 'root',
  host: 'localhost',
  database: 'wx_uv',
  password: 'SailView2627!',
  port: 5432,
  max: 500,
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 120000,
  maxUses: 100,
})

async function getUvDb() {
  return uvDbPool
}

const stnPool = new Map();
async function getOtherDb(stnName){
   if(!stnPool.has(stnName)){
    stnPool.set(stnName,  new Pool({
      host: 'localhost',
      database: stnName.toLowerCase(),
      user: 'root',
      password: 'SailView2627!',
      port: 5432,
      max: 50,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: 120000,
      maxUses: 100,
    }))
   }
   return stnPool.get(stnName);
}

const noaaNwsPool = new Pool({
  user: 'root',
  host: '127.0.0.1',
  database: 'stn_other',
  password: 'SailView2627!',
  port: 5432,
  max: 100, 
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 120000, 
  maxUses: 100,
})

async function getNoaaNwsDb() {
  return noaaNwsPool
}

module.exports = {
  getAdminDb,
  getOtherDb,
  getUvDb,
  getNoaaNwsDb,
}
