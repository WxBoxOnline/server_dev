const db = require("./db")
const _ = require('underscore')
const funcs = require('./funcs')

async function getStationStatus(socket) {
    var admDb = await db.getAdminDb()
    try{
        const stns = await admDb.query('SELECT * FROM wx_stations ORDER BY wx_sid ASC')//,function(err, stns) {
            // if (err) {
            //     funcs.logMessage("adm","getStationStatus: ERROR => "+err);
            //     return;
            // }
            var stnData = new Array()
            for( var i = 0; i < stns.rowCount; i++ ) {
                var stn = stns.rows[i];

                var tData = {}

                tData["wx_sid"] = stn.wx_sid
                tData["wbox_version"] = stn.wbox_version
                tData["wbox_device"] = stn.wbox_device
                tData["location"] = stn.location
                tData["stn_name"] = stn.stn_name
                tData["stn_lat"] = stn.stn_lat
                tData["stn_lon"] = stn.stn_lon
                tData["dataflow"] = stn.dataflow
                tData["connected"] = stn.connected
                tData["owner"] = stn.owner

                stnData.push(tData)
            }

            var outData = {}
            outData['stn_count'] = stns.rowCount 
            outData['stn_data'] = stnData

            outMsg = funcs.formatMsg("adm","stn_status",outData)
            socket.send(outMsg)
        // })
    } catch (err) {
        funcs.logMessage("adm","getStationStatus: ERROR => "+err);
    }
}

async function checkOfflineStns(stnsSkt) {
    var admDb = await db.getAdminDb()
    try {
        const stns = await admDb.query("SELECT * FROM wx_stations ORDER BY wx_sid ASC")//,async function(err, stns) {
            // if (err) {
            //     funcs.logMessage("adm","checkOfflineStns: ERROR => "+err);
            //     return;
            // }
        
            for( var i = 0; i < stns.rowCount; i++ ) {
                var stn = stns.rows[i]

                var stnDb = await db.getOtherDb(stn.wx_sid)
                if( stnDb != -1 ) {
                    const dbT = await stnDb.connect()
                    try{ 
                        var stnData = await dbT.query("SELECT * FROM wx_data ORDER BY wx_msec DESC LIMIT 1")
                        var stnLast = parseInt(stnData.rows[0].wx_msec)

                        var nowDate = new Date()
                        var nowMsec = Math.round(nowDate.getTime()/1000)

                        if( (nowMsec - stnLast) >= 120 ) {
                            if( stn.dataflow == '1' ) {
                                var updSql = "UPDATE wx_stations SET "
                                updSql += "dataflow='0' "
                                updSql += "WHERE wx_sid='"+stn.wx_sid+"'"
                            
                                funcs.logMessage(stn.wx_sid,"checkOfflineStns: Setting Data Fail ("+nowMsec+" -> "+stnLast+")")

                                admDb.query(updSql)

                                if( stnsSkt[stn.wx_sid] != null ) {
                                    // need to grab the latest log file from remote before restarting
                                    funcs.logMessage(stn.wx_sid,"checkOfflineStns: Requesting Log File")
                                    wxOut = {
                                        'wx_sid': stn.wx_sid,
                                    }
                                    const outMsg = funcs.formatMsg('svr','uploadLog',wxOut)
                                    stnsSkt[stn.wx_sid].send(outMsg)

                                    funcs.logMessage(stn.wx_sid,"checkOfflineStns: Restarting Service")

                                    wxOut = {
                                        'wx_sid': stn.wx_sid,
                                    }
                                    outMsg = funcs.formatMsg('svr','restartThreads',wxOut)
                                    stnsSkt[stn.wx_sid].send(outMsg)
                                }
                            }
                        } else {
                            if( stn.dataflow == '0' ) {  
                                var updSql = "UPDATE wx_stations SET "
                                updSql += "dataflow='1' "
                                updSql += "WHERE wx_sid='"+stn.wx_sid+"'"
                            
                                funcs.logMessage(stn.wx_sid,"checkOfflineStns: Setting Data Ok")

                                admDb.query(updSql) 
                            }
                        }

                    } catch (err) {
                        dbT.release()
                    } finally {
                        dbT.release()
                    }
                }
            }
        // })
        
    } catch (err) {
        funcs.logMessage("adm","checkOfflineStns: ERROR => "+err);
    }
}

async function setStationData(wxSid,dataflow) {
    var admDb = await db.getAdminDb()
    try {
        const stns = await admDb.query("SELECT * FROM wx_stations WHERE wx_sid='"+wxSid+"'")//,function(err, stns) {
            // if (err) {
            //     funcs.logMessage("adm","setStationData: ERROR ("+wxSid+") => "+err);
            //     return;
            // }
            var stn = stns.rows[0]

            if( parseInt(stn.dataflow) != parseInt(dataflow) ) {
                var updSql = "UPDATE wx_stations SET "
                updSql += "dataflow='"+dataflow+"' "
                updSql += "WHERE wx_sid='"+stn.wx_sid+"'"
            
                if( dataflow == "1" ) {
                    funcs.logMessage(stn.wx_sid,"setStationData: Setting Data Ok")
                } else {
                    funcs.logMessage(stn.wx_sid,"setStationData: Setting Data NOT Ok")
                }

                admDb.query(updSql) 
            }
        // })
        
    } catch (err) {
        funcs.logMessage(wxSid,"setStationData: ERROR => "+err);
    }
}

async function setStationConnected(wxSid,connected) {
    var admDb = await db.getAdminDb()
    try{
        const stns = await admDb.query("SELECT * FROM wx_stations WHERE wx_sid='"+wxSid+"'")//,function(err, stns) {
            // if (err) {
            //     funcs.logMessage("adm","setStationConnected: ERROR ("+wxSid+") => "+err);
            //     return;
            // }
            var stn = stns.rows[0]

            if( stn.connected != parseInt(connected) ) {
                var updSql = "UPDATE wx_stations SET "
                updSql += "connected='"+connected+"'"
                if( connected == '0' ) updSql += ",dataflow='0'"
                updSql += " WHERE wx_sid='"+wxSid+"'"

                if( connected == "0" ) {
                    funcs.logMessage(wxSid,"setStationConnected: Setting Disconnected")
                } else {
                    funcs.logMessage(wxSid,"setStationConnected: Setting Connected")
                }

                admDb.query(updSql) 
            }
        // })
        
    } catch (err) {
        funcs.logMessage(wxSid,"setStationConnected: ERROR => "+err);
    }
}

async function setAllStationsDisconnected() {
    var admDb = await db.getAdminDb()
    try{
        const stns = await admDb.query("SELECT * FROM wx_stations ORDER BY wx_sid ASC")//,function(err, stns) {
            // if (err) {
            //     funcs.logMessage("adm","setAllStationsDisconnected: ERROR => "+err);
            //     return;
            // }

            for( var i = 0; i < stns.rowCount; i++ ) {
                var stn = stns.rows[i]
                var updSql = "UPDATE wx_stations SET "
                updSql += "connected='0',dataflow='0' "
                updSql += "WHERE wx_sid='"+stn.wx_sid+"'"

                funcs.logMessage(stn.wx_sid,"setAllStationsDisconnected: Setting Disconnected")


                admDb.query(updSql) 
            }
        // })
        
    } catch (err) {
        funcs.logMessage("adm","setAllStationsDisconnected: ERROR => "+err);
    }
}

module.exports = {
    getStationStatus,
    checkOfflineStns,
    setStationConnected,
    setAllStationsDisconnected,
    setStationData,
}