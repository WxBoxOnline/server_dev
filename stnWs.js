const funcs = require('./funcs')
const stnFuncs = require("./stnFuncs")
const admFuncs = require("./admFuncs")
const nxtFuncs = require("./nxtFuncs")
const WebSocket = require('ws')

const wsServer = new WebSocket.Server({ noServer: true })

var stnRooms = {}
var stns = {}
var stnsSkt = {}

var offlineInterval = setInterval(function() {
    admFuncs.checkOfflineStns(stnsSkt)
},15000)

wsServer.on('connection', socket => {
    socket.id = Date.now()
    socket.on('message', msgData => {
        const wxMsg = JSON.parse(msgData)

        if( wxMsg.dataType == "stn" ) {
            processStnMessage(wxMsg,socket)
        } else if( wxMsg.dataType == "clt" ) {
            processCltMessage(wxMsg,socket)
        } else if (wxMsg.dataType == "adm" ) {
            processAdmMessage(wxMsg,socket)
        } else if (wxMsg.dataType == "nxt" ) {
            processNxtMessage(wxMsg,socket)
        }
    })
    socket.on('close', () => {
        if( !(socket.id in stns) ) {
            processCltClose(socket)
        } else {
            processStnClose(socket)
        }
    })
    socket.on('disconnect', () => {
        if( !(socket.id in stns) ) {
            processCltClose(socket)
        } else {
            processStnClose(socket)
        }
    })
    socket.on('error', () => {
        if( !(socket.id in stns) ) {
            processCltClose(socket)
        } else {
            processStnClose(socket)
        }
    })
    
    const outMsg = funcs.formatMsg('svr','ack','')
    socket.send(outMsg)
})

const processStnClose = (socket) => {
    var stnId = stns[socket.id]
    for( var i = 0; i < stnRooms[stnId].length; i++ ) {
        var clt = stnRooms[stnId][i]

        stnsSkt[stnId] = null
        stns[socket.id] = null

        const outMsg = funcs.formatMsg('clt','offline','')
        clt['socket'].send(outMsg)
    }

    funcs.logMessage(stnId,"processStnClose: Stn Disconnect => "+socket.id)
    admFuncs.setStationConnected(stnId,'0')
}

const processCltClose = (socket) => {
    for( stnId in stnRooms ) {
        var cltFound = false
        for( var i = 0; i < stnRooms[stnId].length; i++ ) {
            var clt = stnRooms[stnId][i]
            if( clt['socket'] == socket ) {
                funcs.logMessage(stnId,"processCltClose: Clt Disconnect => "+socket.id)
                stnRooms[stnId].splice(i,1)
                funcs.logMessage(stnId,"processCltClose: Total Clients in Room => "+stnRooms[stnId].length)
                cltFound = true
                break
            }
        }
        if( cltFound ) break
    }
}

async function processNxtMessage(wxMsg,socket) {
    const wxData = wxMsg.data

    if( wxMsg.action == "wxWind" ) {
        //nxtFuncs.getNxtWind(wxData,socket)
    }
}

async function processStnMessage(wxMsg,socket) {
    const wxData = wxMsg.data

    if( wxMsg.action == "subscription" ) {
        stnFuncs.checkStnSubscription(wxData.sid,socket,stnsSkt,"svr")
        
        admFuncs.setStationConnected(wxData.sid,'1')
    } else if( wxMsg.action == "stnInfo" ) {
        stnFuncs.updateStnInfo(wxData,socket)
    } else if( wxMsg.action == "update" ) {
        stnFuncs.checkStnUpdate(wxData,socket)
    } else if( wxMsg.action == "register" ) {
        funcs.logMessage(wxData.stnId,"Stn Connect => "+socket.id)
        processStnRegister(wxData,socket)
    } else if( wxMsg.action == "latest" ) {
        stnFuncs.getLatestWxData(wxData.wx_sid,socket)
    } else if( wxMsg.action == "stnUv" ) {
        processStnUvRequest(wxData.wx_sid,socket)
    } else if( wxMsg.action == "wx_db" ) {
        processStnWx(wxData,socket)
    }
}

const processStnUvRequest = (stnId,socket) => {

}

const processStnWx = (wxData,socket) => {
    if( Object.keys(stnRooms).length > 0 ) {
        if( wxData.wx_sid in stnRooms ) {
            for( var i = 0; i < stnRooms[wxData.wx_sid].length; i++ ) {
                var clt = stnRooms[wxData.wx_sid][i]

                if( wxData.wx_data == "evt_lightning") funcs.logMessage(wxData.wx_sid,"processStnWx: Sending data: "+wxData.wx_data+" to "+clt['socket'].id)

                const outMsg = funcs.formatMsg('clt','wx',wxData)
                clt['socket'].send(outMsg)
            }
        }
    }

    //push incoming wx data to database
    stnFuncs.insertWxData(wxData)
}

const processStnRegister = (wxData,socket) => {
//need to ensure xref data is up to date...
    if( Object.keys(stnRooms).length > 0 ) {
        if( !(wxData.stnId in stnRooms) ) {
            stnRooms[wxData.stnId] = new Array()
            funcs.logMessage(wxData.stnId,"processStnRegister: Room Created")
        } else {
            funcs.logMessage(wxData.stnId,"processStnRegister: Has Room")
        }
    } else {
        stnRooms[wxData.stnId] = new Array()
        funcs.logMessage(wxData.stnId,"processStnRegister: Room Created")  
    }

    stns[socket.id] = wxData.stnId
    // stnsSkt[wxData.stnId] = socket
    // funcs.logMessage(wxData.stnId,"processStnRegister: Socket => ."+stnsSkt[wxData.stnId].id) 

    stnFuncs.updateStnXref(wxData)
    
    const outMsg = funcs.formatMsg('svr','register','')
    socket.send(outMsg)
}

async function processCltMessage(wxMsg,socket) {
    const wxData = wxMsg.data

    var sid 
    if( 'wx_sid' in wxData ) {
        sid = wxData.wx_sid
    } else if( 'stnId' in wxData ) {
        sid = wxData.stnId
    } else if( 'station_id' in wxData ) {
        sid = wxData.station_id
    } else if( 'sid' in wxData ) {
        sid = wxData.sid
    } else {
        funcs.logMessage(sid,"processCltMessage: SID not found => "+JSON.stringify(wxData)) 

        Object.keys(stnRooms).forEach(stnId =>  {
            funcs.logMessage(stnId,"processCltMessage: Checking Room for client") 
            for( var i = 0; i < stnRooms[stnId].length; i++ ) {
                var clt = stnRooms[stnId][i]
                if( clt['socket'] == socket ) {
                    funcs.logMessage(stnId,"processCltMessage: Found client in Room") 
                    sid = stnId
                    break
                }
            }
        })
    }

    if( wxMsg.action == "subscription" ) {
        stnFuncs.checkStnSubscription(sid,socket,null,"clt")
    } else if( wxMsg.action == 'signup' ) {
        funcs.logMessage('ADMIN',"processCltMessage: Total Rooms => "+Object.keys(stnRooms).length)
        var cltFound = false
        if( Object.keys(stnRooms).length > 0 ) {
            funcs.logMessage('ADMIN',"processCltMessage: Finding Room => "+sid)

            if( sid in stnRooms ) {
                for( var i = 0; i < stnRooms[sid].length; i++ ) {
                    var clt = stnRooms[sid][i]
                    if( clt['socket'] == socket ) {
                        cltFound = true
                    } else {    
                        const outMsg = funcs.formatMsg('clt','new','')
                        clt['socket'].send(outMsg)
                    }
                }
                if( !cltFound ) {
                    var clt = {};
                    clt['socket'] = socket;
                    clt['signupTime'] = Date.now()
                    stnRooms[sid].push(clt)
                    funcs.logMessage('ADMIN',"processCltMessage: Added Client to "+sid+" Room => "+socket.id)
                } else {
                    funcs.logMessage('ADMN',"processCltMessage: Client in room already")
                }
                funcs.logMessage('ADMIN',"processCltMessage: Total Clients in "+sid+" Room => "+stnRooms[sid].length)

                var stnData = await stnFuncs.getStationInfo(sid,"1")

                const outMsg = funcs.formatMsg('clt','signup',stnData)
                socket.send(outMsg)
            } else {
                funcs.logMessage('ADMIN',"processCltMessage: Room Does Not Exist ("+sid+")...")
                data = {
                    'stn':"-1"
                }
                const outMsg = funcs.formatMsg('clt','signup',data)
                socket.send(outMsg)
            }
        } else {
            funcs.logMessage('ADMIN',"processCltMessage: No Rooms Exist ("+sid+")...")
            //var stnData = await stnFuncs.getStationInfo(sid,"1")
            var stnData = {
                'stn':"-2"
            }

            const outMsg = funcs.formatMsg('clt','signup',stnData)
            socket.send(outMsg)
        }
    } else if( wxMsg.action == "latest" ) {
        stnFuncs.getLatestWxData(sid,socket)
    } else if( wxMsg.action == "wxBrg10" ) {
        stnFuncs.getWxBrg10(sid,socket)
    } else if( wxMsg.action == "graphData" ) {
        stnFuncs.getGraphData(wxData,sid,socket)
    } else if( wxMsg.action == "webUpdate" ) {
        stnFuncs.checkWebUpdate(wxData,sid,socket)
    } else if( wxMsg.action == "nws_stns" ) {
        stnFuncs.getClosestStations_NWS(wxData,socket)
    } else if( wxMsg.action == "noaa_stns" ) {
        stnFuncs.getClosestStations_NOAA(wxData,socket)
    } else if( wxMsg.action == "noaa_obs" ) {
        stnFuncs.getNoaaObs(wxData,socket)
    } else if( wxMsg.action == "nws_obs" ) {
        stnFuncs.getNwsObs(wxData,socket)
    } else if( wxMsg.action == "nws_alerts" ) {
        stnFuncs.getNwsAlerts(wxData,socket)
    }
}

const processAdmMessage = (wxMsg,socket) => {
    if( wxMsg.action == "stn_status" ) {
        admFuncs.getStationStatus(socket)
    }
}

module.exports = {
    wsServer,
    stnRooms,
    stnsSkt,
}