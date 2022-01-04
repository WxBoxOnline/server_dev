const express = require('express')
const bodyParser = require('body-parser')
const http = require('http')
const cron = require('node-cron')
const app = express()
const port = 32900

const stnWs = require("./stnWs")
const stnFuncs = require('./stnFuncs')
const admFuncs = require('./admFuncs')
const nxtFuncs = require('./nxtFuncs')
const funcs = require("./funcs")
// const noaa = require("./noaa")

const fs = require('fs')

global.tzQuery = false

if (fs.existsSync(funcs.logFile)) {
  const stats = fs.statSync(funcs.logFile)
  // var fileDate = stats.mtime.getDate()
  // var fileSecs = Math.round(stats.mtime.getTime() / 1000)

  const today = new Date()
  var nowDate = today.getDate()
  var nowSecs = Math.round(today.getTime() / 1000)

  //if( fileDate < nowDate ) {
      fs.copyFile(funcs.logFile,"/home/beenth12/www/ws/wxbox/node/log/"+stats.mtime.getFullYear()+"."+(stats.mtime.getMonth()+1)+"."+stats.mtime.getDate()+"_"+nowSecs+".log",fs.constants.COPYFILE_FICLONE,function(){})
      fs.unlink(funcs.logFile,function(){})
  //}
}

app.use(bodyParser.json())
app.use(
  bodyParser.urlencoded({
    extended: true,
  })
)

funcs.logMessage('STARTUP',`================================================================================`)
funcs.logMessage('STARTUP',`Starting WxBoxNode`)
funcs.logMessage('STARTUP',`================================================================================`)

// var current_ts = Math.floor(Date.now() / 1000) 
// try {
//   noaa.getNoaaStations(current_ts) 
// }
// catch(e) {
//   console.log('Catch an error: ', e)
// }

// try {
//   noaa.getNoaaObs(current_ts) 
// }
// catch(e) {
//   console.log('Catch an error: ', e)
// }

// cron.schedule('*/5 * * * *', function() {
//   try {
//     var current_ts = Math.floor(Date.now() / 1000)  
//     noaa.getNoaaObs(current_ts)  
//   }
//   catch(e) {
//     console.log('Catch an error: ', e)
//   }
// })
// cron.schedule('0 * * * *', function() {
//   try {
//     var current_ts = Math.floor(Date.now() / 1000) 
//     noaa.getNoaaStations(current_ts)  
//   }
//   catch(e) {
//     console.log('Catch an error: ', e)
//   }
// })

cron.schedule('*/5 * * * *', function() {
  try {
    stnFuncs.checkStnUpdateAll(stnWs.stnsSkt)  
  }
  catch(e) {
    console.log('Catch an error: ', e)
  }
})

const server = http.createServer(app)
server.listen(port)
server.on('listening', function() {
  var outMsg = 'WxBoxNode started on '+port;
  funcs.logMessage('STARTUP',outMsg);
})

app.get('/', (request, response) => {
  response.json({info: 'Welcome to WeatherBox(sm)' })
})

app.get('/stations', stnFuncs.getAllStations)
app.get('/stations/:id',stnFuncs.getStationBySid)
app.get('/maxUv',stnFuncs.getWebUv)
app.get("/nxt",nxtFuncs.testCoord)
app.listen(32627)

server.on('upgrade', (request, socket, head) => {
  stnWs.wsServer.handleUpgrade(request, socket, head, socket => {
    stnWs.wsServer.emit('connection', socket, request);
  });
})

admFuncs.setAllStationsDisconnected()