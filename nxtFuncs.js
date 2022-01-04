const db = require("./db")
const funcs = require('./funcs')
const admFuncs = require("./admFuncs")

async function testCoord(request, response) {
    var wxLat = 30.4073799
    var wxLon = -86.4730725

    var wxData = {
        wxLat:wxLat,
        wxLon:wxLon
    }

    winds = await getNxtWind(wxData,null)

    //funcs.logMessage("NXT","testCoord: Data =>  "+JSON.stringify(winds))

    response.json(JSON.stringify(winds))
}

async function getNxtWind(wxData,socket) {
    var llCoord = []
    var urCoord = []
    if( ('wxLat' in wxData) && ('wxLon' in wxData) ) {
        llCoord = funcs.calcBoundingCoords(wxData.wxLat,wxData.wxLon,10,'m',225)
        urCoord = funcs.calcBoundingCoords(wxData.wxLat,wxData.wxLon,10,'m',45)
    } else if ( ('llLat' in wxData) && ('llLon' in wxData) &&  ('urLat' in wxData) && ('urLon' in wxData)) {
        llCoord['wxLat'] = parseFloat(wxData.llLat)
        llCoord['wxLon'] = parseFloat(wxData.llLon)
        urCoord['wxLat'] = parseFloat(wxData.urLat)
        urCoord['wxLon'] = parseFloat(wxData.urLon)
    } else {
        llCoord['wxLat'] = 30.4
        llCoord['wxLon'] = -86.4
        urCoord['wxLat'] = 30.5
        urCoord['wxLon'] = -86.3
    }
    
    // funcs.logMessage("NXT","getNxtWind: Data =>  "+JSON.stringify(wxData))
    
    // funcs.logMessage("NXT","getNxtWind: llCoord =>  "+JSON.stringify(llCoord))
    
    // funcs.logMessage("NXT","getNxtWind: urCoord =>  "+JSON.stringify(urCoord))

    var wxTime = 60
    if( 'wxTime' in wxData ) {
        wxTime = parseint(wxData.wxTime)
    }

    var nowDate = Date.now()
    var oldTime = Math.floor(nowDate / 1000) - (wxTime * 60)

    var admDb = await db.getAdminDb()
    const admT = await admDb.connect()
    var wxStns
    try {

        try {
            dbSql = "SELECT * FROM wx_stations WHERE stn_lat BETWEEN "+llCoord['wxLat']+" AND "+urCoord['wxLat']+" AND stn_lon BETWEEN "+llCoord['wxLon']+" AND "+urCoord['wxLon']
            dbRows = await admT.query(dbSql)

            var winds = new Array()
            for( var i = 0; i < dbRows.rowCount; i++ ) {
                var stn = dbRows.rows[i]

                var stnDb = await db.getOtherDb(stn.wx_sid.toLowerCase())
                if( stnDb != -1 ) {
                    var stnT =await stnDb.connect()
                    try {
                        wxSql = "SELECT * FROM wx_data WHERE wx_type = 'rw' AND wx_msec >= "+oldTime+" AND wx_wind_avg!='-999' AND wx_bearing!='-999' ORDER BY wx_msec ASC"
                        wxRows = await stnT.query(wxSql)

                        for( var j = 0; j < wxRows.rowCount; j++ ) {
                            var wind = wxRows.rows[j]
                            var tWind = {
                                stnid:wind.wx_sid,
                                wSpd: wind.wx_wind_avg,
                                wBrg: wind.wx_bearing,
                                wMsec:wind.wx_msec,
                                wLat: stn.stn_lat,
                                wLon: stn.stn_lon
                            }
                            winds.push(tWind)
                        }
                    } catch(err) {
                        funcs.logMessage("NXT","getNxtWind: ERROR =>  "+err)
                    } finally {
                        stnT.release
                    }
                }
            }

            wxOut = {
                'winds': winds
            }

            if( socket != null ) {
                const outMsg = funcs.formatMsg('nxt','wxWind',wxOut)
                socket.send(outMsg)
            } else {
                return winds
            }
            
        } catch(e) {
            funcs.logMessage('NXT',"getNxtWind: ERROR => "+e)
            funcs.logMessage('NXT',"getNxtWind: ERROR => "+maxSql)
        }
        
    } catch (err) {
        funcs.logMessage("NXT","getNxtWind: ERROR =>  "+err)
    } finally {
        admT.release()
    }
}

module.exports = {
    getNxtWind,
    testCoord,
}