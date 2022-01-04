const db = require("./db")
const _ = require('underscore')
const fetch = require('node-fetch')
const funcs = require('./funcs')
const admFuncs = require("./admFuncs")
const luxon = require('luxon')
const MeeusSunMoon = require('meeussunmoon')
const fs = require('fs')

const nws = require('weathered')
const { strictEqual } = require("assert")

const tzKey = "2GH3N2CAK8LI"

var noaaApi = "EweXvCWyrpHcegveVsOVWROWRgmRfaOF"

async function getClosestStations_NWS(wxData,socket) {
    try {
        const nwsClient = new nws.Client()
        const stations = await nwsClient.getStations(wxData.stnLat, wxData.stnLon)
    
        if( typeof stations !== 'undefined' ) {
            var stnDataOutT = new Array()

            stations.features.forEach(async function(station) {
                var stnDataT = {}
                stnDataT['stnType'] = "NWS"
                stnDataT['stnId'] = station.properties.stationIdentifier
                stnDataT['stnName'] = station.properties.name

                stnDataOutT.push(JSON.stringify(stnDataT))
            })

            var stnDataOut = {}
            stnDataOut['stns'] = stnDataOutT
            outMsg = funcs.formatMsg("clt","nws_stns",stnDataOut)
            socket.send(outMsg)
        } else {
            funcs.logMessage(wxData.wx_sid,"getClosestStations_NWS: No Stations Found")
        }
        
    } catch (err) {
        funcs.logMessage(wxData.station_id,"getClosestStations_NWS: ERROR =>  "+err)
    } 
}

async function getNwsObs(wxData,socket) {
    const nwsClient = new nws.Client()

    const latestObservation = await nwsClient.getLatestStationObservations(wxData.station_id)
    if( typeof latestObservation !== 'undefined' ) {
        outMsg = funcs.formatMsg("clt","nws_obs",latestObservation.properties)
        socket.send(outMsg)
    } else {
        funcs.logMessage(wxData.wx_sid,"getNwsObs: Station not found ("+wxData.station_id+")")
    }
}

async function getNwsAlerts(wxData,socket) {
    const nwsClient = new nws.Client()

    latitude = wxData.stnLat
    longitude = wxData.stnLon

    const latestAlerts = await nwsClient.getAlerts(true,{latitude,longitude})
    if( typeof latestAlerts !== 'undefined' ) {
        outMsg = funcs.formatMsg("clt","nws_alerts",latestAlerts)
        socket.send(outMsg)
    } else {
        funcs.logMessage(wxData.wx_sid,"getNwsAlerts: Alerts not found ("+latitude+","+longitude+")")
    }
}

async function getClosestStations_NOAA(wxData,socket) {
    try{
        var myLatLow = parseFloat(wxData.stnLat)-0.5
        var myLatHigh =  parseFloat(wxData.stnLat)+0.5
        var myLonLow = parseFloat(wxData.stnLon)-0.5
        var myLonHigh =  parseFloat(wxData.stnLon)+0.5

        var wxDb = await db.getNoaaNwsDb()
        const wxDbT = await wxDb.connect()

        try {
            var apiQry = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?limit=25&sortfield=name&extent="+myLatLow+","+myLonLow+","+myLatHigh+','+myLonHigh
            const apiHeader = {
                'Token':noaaApi
            }

            const noaaResp = await fetch(apiQry,{headers: {'Token':noaaApi}})
            if(noaaResp.ok){
                const noaaStns = await noaaResp.json()

                var stnDataOutT = new Array()
                for( var i = 0; i < noaaStns.results.length; i++ ) {
                    var stnDataT = {}
                    stnDataT['stnType'] = "NOAA"
                    stnDataT['stnId'] = noaaStns.results[i].id.toUpperCase()
                    stnDataT['stnName'] = noaaStns.results[i].name
                    
                    stnDataOutT.push(JSON.stringify(stnDataT))
                }
                var stnDataOut = {}
                stnDataOut['stns'] = stnDataOutT
                outMsg = funcs.formatMsg("clt","noaa_stns",stnDataOut)
                socket.send(outMsg)
            } else {
                funcs.logMessage(wxData.station_id,"getClosestStations_NOAA: ERR => ("+noaaResp.status+") "+noaaResp.statusText)
            }
        } catch (err) {
            funcs.logMessage(wxData.station_id,"getClosestStations_NOAA: ERROR =>  "+err)
        } finally {
            wxDbT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getClosestStations_NOAA: ERROR => "+e)
        funcs.logMessage(wxSid,"getClosestStations_NOAA: ERROR => "+maxSql)
    }
}

async function getNoaaObs(wxData,socket) {
    try{
        funcs.logMessage(wxData.wx_sid,"getNoaaObs: Startup ("+wxData.station_id+")")

        var wxDb = await db.getNoaaNwsDb()
        const wxDbT = await wxDb.connect()

        try {
            var getSql = "SELECT * FROM wx_obs WHERE stn_id='"+wxData.station_id+"'"

            var stnInfo = ''
            stnInfo = await wxDbT.query(getSql)
            if( stnInfo.rowCount > 0 ) {
                
                var stnDataOutT = new Array()

                var stns = stnInfo.rows

                for( var i = 0; i < stnInfo.rowCount; i++ ) {
                    var stnDataT = {}
                    stnDataT['stnType'] = "NOAA"
                    stnDataT['stnId'] = stns[i].stn_id.toUpperCase()
                    stnDataT['stnName'] = stns[i].stn_name
                    
                    stnDataOutT.push(JSON.stringify(stnDataT))
                }

                var stnDataOut = {}
                stnDataOut['stns'] = stnDataOutT
                outMsg = funcs.formatMsg("clt","noaa_stns",stnDataOut)
                socket.send(outMsg)
            }
        } catch (err) {
            funcs.logMessage(wxData.station_id,"getClosestStations_NOAA: ERROR =>  "+err)
        } finally {
            wxDbT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getNoaaObs: ERROR => "+e)
        funcs.logMessage(wxSid,"getNoaaObs: ERROR => "+maxSql)
    }
}

function timeout(ms,myFunc,wxSid) {
    //funcs.logMessage(myFunc+" ("+wxSid+") waiting")
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function getAllStations (request, response) {
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            var stns = admT.query('SELECT * FROM wx_stations ORDER BY wx_sid ASC')
            response.status(200).json(stns.rows)
            
        } catch (err) {
            funcs.logMessage("WEB","getAllStations: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getAllStations: ERROR => "+e)
        funcs.logMessage(wxSid,"getAllStations: ERROR => "+maxSql)
    }
}

async function getWebUv(request,response) {
    const uvData = request.query

    if( uvData.tz == undefined ) {
        uvData.tz = 0
    }
    if( uvData.tm == undefined ) {
        uvData.tm = 0
    }
    
    funcs.logMessage('ADMIN',"getWebUv: coordsLat => "+uvData.lat)
    funcs.logMessage('ADMIN',"getWebUv: coordsLon => "+uvData.lon)
    funcs.logMessage('ADMIN',"getWebUv: coordsLon => "+uvData.tz)
    funcs.logMessage('ADMIN',"getWebUv: coordsLon => "+uvData.tm)

    var myUv = await getMaxUv(response,uvData.lat,uvData.lon,uvData.tz,uvData.tm)

    response.status(200).json(myUv)
}
  
async function getStationBySid(request, response) {
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            const sid = request.params.id.toUpperCase()

            var stnInfo = ''

            stnInfo = await admT.query('SELECT * FROM wx_stations WHERE wx_sid=$1',[sid])

            response.status(200).json(stnInfo.rows)
            
        } catch (err) {
            funcs.logMessage(wx_sid,"getStationBySid: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getStationBySid: ERROR => "+e)
        funcs.logMessage(wxSid,"getStationBySid: ERROR => "+maxSql)
    }
}

async function checkStnUpdateAll(stnsSkt) {
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            var verRes = await admT.query('SELECT * FROM wb_versions')

            var verData = verRes.rows
            
            //need to grab all active stations
            var activeRes = await admT.query('SELECT * FROM wx_stations WHERE connected=1')
            
            if( activeRes.rowCount > 0 ) {
                for( var i = 0; i < activeRes.rowCount; i++ ) {
                    var activeStn = activeRes.rows[i]
                    var stnVerParts = activeStn.wbox_version.split('.')
                    var svrVerParts = ""
                    var updRequired = false
                    if( activeStn.wbox_device.toUpperCase().includes("LINUX") ) {
                        svrVerParts =  verData[0].linux.split('.')
                    } else if( activeStn.wbox_device.toUpperCase().includes("WINDOWS") ) {
                        svrVerParts =  verData[0].windows.split('.')
                    }

                    if( parseInt(stnVerParts[0]) < parseInt(svrVerParts[0]) ) {
                        updRequired = true
                    } else if( parseInt(stnVerParts[0]) == parseInt(svrVerParts[0]) ) {
                        if( (parseInt(stnVerParts[1]) < parseInt(svrVerParts[1])) ) {
                            updRequired = true
                        } else if ( parseInt(stnVerParts[1]) == parseInt(svrVerParts[1]) ) {
                            if( (parseInt(stnVerParts[2]) < parseInt(svrVerParts[2])) ) {
                                updRequired = true
                            }
                        }
                    }

                    var outData 
                    if( updRequired ) {
                        funcs.logMessage(activeStn.wx_sid,"checkStnUpdateAll: Station requires an update.")     
                        outData = {
                            'upd':'1'
                        }

                        //now, find station websocket to send data to
                        if( stnsSkt[activeStn.wx_sid] != null ) {
                            funcs.logMessage(activeStn.wx_sid,"checkStnUpdateAll: Sending update message.")  
                            var outMsg = funcs.formatMsg("svr","update",outData)
                            stnsSkt[activeStn.wx_sid].send(outMsg)
                        } else {
                            funcs.logMessage(activeStn.wx_sid,"checkStnUpdateAll: Socket NULL.")    
                        }
                    }

                }
            }


            
        } catch (err) {
            funcs.logMessage("ADMIN","checkStnUpdateAll: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage("ADMIN","checkStnUpdateAll: ERROR => "+e)
    }
}

async function checkStnUpdate(wxData,socket) {
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            var verRes = await admT.query('SELECT * FROM wb_versions')

            var verData = verRes.rows
            var svrVerParts
            var updRequired = false
            if( wxData.os == 'linux' ) {
                svrVerParts = verData[0].linux.split('.')
            } else if( wxData.os == 'windows' ) {
                svrVerParts = verData[0].windows.split('.')
            } 

            var stnVer = wxData.wbVersion
            stnVerParts = stnVer.split(".")
            
            if( parseInt(stnVerParts[0]) < parseInt(svrVerParts[0]) ) {
                updRequired = true
            } else if( parseInt(stnVerParts[0]) == parseInt(svrVerParts[0]) ) {
                if( (parseInt(stnVerParts[1]) < parseInt(svrVerParts[1])) ) {
                    updRequired = true
                } else if ( parseInt(stnVerParts[1]) == parseInt(svrVerParts[1]) ) {
                    if( (parseInt(stnVerParts[2]) < parseInt(svrVerParts[2])) ) {
                        updRequired = true
                    }
                }
            }
            
            var outData 
            if( updRequired ) {
                funcs.logMessage(wxData.sid,"checkStnUpdate: Station requires an update.")     
                outData = {
                    'upd':'1'
                }
            } else {
                outData = {
                    'upd':'0'
                }
            }

            var outMsg = funcs.formatMsg("svr","update",outData)
            socket.send(outMsg)
            
        } catch (err) {
            funcs.logMessage(wxData.wx_sid,"checkStnUpdate: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"checkStnUpdate: ERROR => "+e)
    }

}

async function checkStnSubscription(wx_sid,socket,stnsSkt,sktType) {
    const sid = wx_sid.toUpperCase()
    //funcs.logMessage(sid,"checkStnSubscription");

    var stnSubStatus = '0'
    var wxOut
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            var subRes = await admT.query('SELECT * FROM wx_station_subs WHERE stnId=$1',[sid])

            if( subRes.rowCount == 1 ) {
                var subRows = subRes.rows

                var cntRes = await admT.query('SELECT * FROM wx_stations WHERE wx_sid=$1',[sid])

                if( cntRes.rowCount == 0 ) {
                    try {
                        var insRes = await admT.query('INSERT INTO wx_stations (wx_sid) VALUES ($1) RETURNING *',[sid])
                        try { 
                            var tblRes =  await admT.query('SELECT FROM pg_database WHERE datname = $1',[sid.toLowerCase()])
                            if( tblRes.rowCount == 0 ) {
                                try { 
                                    var tblSql = 'CREATE DATABASE '+(sid.toLowerCase())+' WITH TEMPLATE stn_template'
                                    await admT.query(tblSql)

                                    tblSql = 'ALTER DATABASE '+sid.toLowerCase()+' OWNER TO postgres'
                                    await admT.query(tblSql)
                            
                                } catch(e) {
                                    funcs.logMessage(sid,"checkStnSubscription: ERROR0 => "+e);
                                }
                            }
                        
                        } catch(e) {
                            funcs.logMessage(sid,"checkStnSubscription: ERROR1 => "+e);
                        }
                        
                    } catch(e) {
                        funcs.logMessage(sid,"checkStnSubscription: ERROR2 => "+e);
                    }
                }

                var nowDate = new Date()
                var endDate = new Date(subRows[0].enddate)

                if( (nowDate.getTime() > endDate.getTime()) && (subRows[0].subid != '9') ) {
                    try {
                        var updRes = await admT.query('UPDATE wx_station_subs SET subStatus=0 WHERE stnId=$1',[sid])

                        stnSubStatus = '0'
                        
                    } catch(e) {
                        funcs.logMessage(sid,"checkStnSubscription: ERROR3 => "+e);
                    }
                } else {
                    stnSubStatus = '1'
                    if( sktType == "svr" ) {
                        stnsSkt[sid] = socket
                    }
                }
                try {
                    var subRes2 = await admT.query('SELECT * FROM wx_subscription WHERE subId=$1',[subRows[0].subid])
                    var subData2 = subRes2.rows
                    
                    var stnRes = await admT.query('SELECT * FROM wx_stations WHERE wx_sid=$1',[sid])
                    var stnData = stnRes.rows

                    var subType = subData2[0].subdesc
                    var subTypeName = subData2[0].subname

                    wxOut = {
                        'subId': subRows[0].subid,
                        'wx_sid': sid,
                        'subStatus': stnSubStatus,
                        'startDate': subRows[0].startdate,
                        'endDate': subRows[0].enddate,
                        'subType': subType,
                        'subName': subTypeName,
                        'trial': subRows[0].trial,
                        'stnName':stnData[0].stn_name,
                        'stnTz':stnData[0].timezone
                    }

                    const outMsg = funcs.formatMsg('svr','subscription',wxOut)
                    socket.send(outMsg)
                    
                } catch(e) {
                    funcs.logMessage(sid,"checkStnSubscription: ERROR4 => "+e);
                }

            }
            
        } catch (err) {
            funcs.logMessage(wx_sid,"checkStnSubscription: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"checkStnSubscription: ERROR => "+e)
        funcs.logMessage(wxSid,"checkStnSubscription: ERROR => "+maxSql)
    }
}

const getStationUvBase = (wx_sid) => {
    const sid = wx_sid.toUpperCase()
    funcs.logMessage(sid,"getStationUvBase: "+sid)

    var stnSubStatus = '0'
    var wxOut
    try{
        var admDb = db.getAdminDb()
        const admT = admDb.connect()
        try {
            var i = 0;
        } catch (err) {
            funcs.logMessage(wx_sid,"getStationUvBase: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getStationUvBase: ERROR => "+e)
        funcs.logMessage(wxSid,"getStationUvBase: ERROR => "+maxSql)
    }
}

async function getStationInfo(wx_sid,stnActive) {
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            var getSql = "SELECT * FROM wx_stations WHERE wx_sid='"+wx_sid.toUpperCase()+"' LIMIT 1"

            var stnInfo = ''
            stnInfo = await admT.query(getSql)
            var stnData = stnInfo.rows[0]

            var getSql2 = "SELECT * FROM wx_station_defs WHERE stn_id='"+wx_sid.toUpperCase()+"' LIMIT 1"

            var stnInfo2 = ''
            stnInfo2 = await admT.query(getSql2)
            if( stnInfo2.rowCount > 0 ) {
                var stnData2 = stnInfo2.rows[0]

                var wxOut = {
                    'stn':stnActive,
                    'owner': stnData.owner,
                    'email': stnData.email,
                    'stnLat': stnData.stn_lat,
                    'stnLon': stnData.stn_lon,
                    'stnName': stnData.stn_name,
                    'stnTz': stnData.timezone,
                    'dataflow': stnData.dataflow,
                    'db_days': stnData.db_days,
                    'wbox_version': stnData.wbox_version,
                    'def_wspd': stnData2.stn_wspd,
                    'def_temp': stnData2.stn_temp,
                    'def_dist': stnData2.stn_dist,
                    'def_baro': stnData2.stn_baro,
                    'def_rain': stnData2.stn_rain,
                    'def_hght': stnData2.stn_hght,
                    'def_view': stnData2.stn_view,
                    'def_skystate': stnData2.stn_skystate,
                    'def_compass': stnData2.stn_compass,
                    'def_nwsstn': stnData2.stn_nwsstn,
                    'def_lightning':stnData2.stn_lightning
                }
            } else {
                var wxOut = {
                    'stn':stnActive,
                    'owner': stnData.owner,
                    'email': stnData.email,
                    'stnLat': stnData.stn_lat,
                    'stnLon': stnData.stn_lon,
                    'stnName': stnData.stn_name,
                    'stnTz': stnData.timezone,
                    'dataflow': stnData.dataflow,
                    'db_days': stnData.db_days,
                    'wbox_version': stnData.wbox_version
                }
            }
            return wxOut
            
        } catch (err) {
            funcs.logMessage(wx_sid,"getStationInfo: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getStationInfo: ERROR => "+e)
        funcs.logMessage(wxSid,"getStationInfo: ERROR => "+maxSql)
    }

}

async function updateStnInfo(wxData,socket) {
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            var offset
            if( 'stn_tzone' in wxData ) {
                offset = wxData.stn_tzone
            } else {
                var checkSql = "SELECT * FROM wx_stations WHERE wx_sid='"+wxData.ws_sid+"'"
                checkRows = await admT.query(checkSql)
                var checkData = checkRows.rows[0]

                offset = checkData.timezone

                if( (offset == "") || (offset == 'null') || (offset == null) || (typeof offset == undefined)) {
                    while( global.tzQuery ) {
                        await timeout(2000,"updateStnInfo",wxData.wx_sid)
                    }
                    global.tzQuery = true
                    var apiQry = "http://api.timezonedb.com/v2.1/get-time-zone?key="+tzKey+"&format=json&by=position&lat="+wxData.stn_lat+"&lng="+wxData.stn_lon
        
                    const apiResponse = await fetch(apiQry)
                    const apiResponseJson = await apiResponse.json()
                    offset = parseInt(apiResponseJson.gmtOffset) / 3600
                }
            }
            
            var updSql = "UPDATE wx_stations SET "
            updSql += "owner='"+wxData.owner+"',"
            updSql += "email='"+wxData.email+"',"
            updSql += "location='"+wxData.location+"',"
            updSql += "wbox_version='"+wxData.version+"',"
            updSql += "wbox_device='"+wxData.device+"',"
            updSql += "local_ip='"+wxData.ip+"',"
            updSql += "db_days='"+wxData.db_days+"',"
            updSql += "stn_lat='"+wxData.stn_lat+"',"
            updSql += "stn_lon='"+wxData.stn_lon+"',"
            updSql += "stn_name='"+wxData.stn_name+"',"
            updSql += "stn_elev='"+wxData.stn_elev+"',"
            updSql += "stn_agl='"+wxData.stn_agl+"',"
            updSql += "timezone='"+offset+"',"
            var nowDate = new Date()
            updSql += "last_msec='"+Math.round(nowDate.getTime()/1000)+"'"

            updSql += " WHERE wx_sid='"+wxData.ws_sid+"'"
            try {
                //funcs.logMessage("updateStnInfo: updSql => "+updSql);
                await admT.query(updSql) 
            } catch(e) {
                funcs.logMessage(wxData.ws_sid,"updateStnInfo: ERROR1 => "+e);
                funcs.logMessage(wxData.ws_sid,"updateStnInfo: ERROR1 => "+updSql);
            }
            global.tzQuery = false
            
        } catch (err) {
            funcs.logMessage(wxData.ws_sid,"updateStnInfo: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"updateStnInfo: ERROR => "+e)
        funcs.logMessage(wxSid,"updateStnInfo: ERROR => "+maxSql)
    }
}

async function updateStnMsec(stnId) {
    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {
            var updSql = "UPDATE wx_stations SET "
            var nowDate = new Date()
            updSql += "last_msec='"+Math.round(nowDate.getTime()/1000)+"'"

            updSql += " WHERE wx_sid='"+stnId+"'"
            try {
                //funcs.logMessage("updateStnMsec: updSql => "+updSql);
                await admT.query(updSql) 
            } catch(e) {
                funcs.logMessage(wxData.ws_sid,"updateStnMsec: ERROR1 => "+e);
                funcs.logMessage(wxData.ws_sid,"updateStnMsec: ERROR1 => "+updSql);
            }
        } catch (err) {
            funcs.logMessage(wxData.ws_sid,"updateStnMsec: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"updateStnMsec: ERROR => "+e)
        funcs.logMessage(wxSid,"updateStnMsec: ERROR => "+maxSql)
    }
}

async function updateStnXref(wxData) {
    try {
        var stnDb = await db.getOtherDb(wxData.stnId)
        if( stnDb != -1 ) {
            const dbT = await stnDb.connect()
            try {
                var getSql = "SELECT * FROM wx_data_xref"
                var xrefRes = await dbT.query(getSql)
                var xrefRowCnt = xrefRes.rows.length

                var xrefSql

                if( xrefRowCnt == 1 ) {
                    var xrefRow = xrefRes.rows[0]
                    xrefSql = "UPDATE wx_data_xref "
                    xrefSql += "SET "
                    xrefSql += "air_in_id='"+wxData.air_in_id+"', "
                    xrefSql += "air_in_name='"+wxData.air_in_name+"', "
                    xrefSql += "air_out_id='"+wxData.air_out_id+"', "
                    xrefSql += "air_out_name='"+wxData.air_out_name+"', "
                    xrefSql += "sky_id='"+wxData.sky_id+"', "
                    xrefSql += "sky_name='"+wxData.sky_name+"', "
                    xrefSql += "rain_id='"+wxData.rain_id+"', "
                    xrefSql += "rain_name='"+wxData.rain_name+"', "
                    xrefSql += "wind_id='"+wxData.wind_id+"', "
                    xrefSql += "wind_name='"+wxData.wind_name+"', "
                    xrefSql += "lightning_id='"+wxData.lightning_id+"', "
                    xrefSql += "lightning_name='"+wxData.lightning_name+"' "
                    xrefSql += "WHERE wx_id="+xrefRow.wx_id
                } else {
                    xrefSql = "INSERT INTO wx_data_xref "
                    xrefSql += "("
                    xrefSql += "air_in_id, "
                    xrefSql += "air_in_name, "
                    xrefSql += "air_out_id, "
                    xrefSql += "air_out_name, "
                    xrefSql += "sky_id, "
                    xrefSql += "sky_name, "
                    xrefSql += "rain_id, "
                    xrefSql += "rain_name, "
                    xrefSql += "wind_id, "
                    xrefSql += "wind_name, "
                    xrefSql += "lightning_id, "
                    xrefSql += "lightning_name"

                    xrefSql += ") VALUES ("

                    xrefSql += "'"+wxData.air_in_id+"', "
                    xrefSql += "'"+wxData.air_in_name+"', "
                    xrefSql += "'"+wxData.air_out_id+"', "
                    xrefSql += "'"+wxData.air_out_name+"', "
                    xrefSql += "'"+wxData.sky_id+"', "
                    xrefSql += "'"+wxData.sky_name+"', "
                    xrefSql += "'"+wxData.rain_id+"', "
                    xrefSql += "'"+wxData.rain_name+"', "
                    xrefSql += "'"+wxData.wind_id+"', "
                    xrefSql += "'"+wxData.wind_name+"', "
                    xrefSql += "'"+wxData.lightning_id+"', "
                    xrefSql += "'"+wxData.lightning_name+"'"
                    
                    xrefSql += ")"
                }

                await dbT.query(xrefSql)

            } catch (err) {
                funcs.logMessage(wxData.stnId,"updateStnXref: ERROR =>  "+err)
            } finally {
                dbT.release()
            }
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"updateStnXref: ERROR => "+e)
        funcs.logMessage(wxSid,"updateStnXref: ERROR => "+maxSql)
    }
}

async function checkWebUpdate(wxData,stnId,socket){
    var verData = await getVersions()

    var htmlParts_svr = verData.html.split(".")
    var jsParts_svr = verData.javascript.split(".")

    var htmlParts_clt = wxData.webVer.split(".")
    var jsParts_clt = wxData.jsVer.split(".")

    var htmlUpdate = false
    var jsUpdate = false

    for(var i = 0; i < htmlParts_svr.length; i++) {
        if( parseInt(htmlParts_svr[i]) > parseInt(htmlParts_clt[i]) ) {
            htmlUpdate = true
            break
        }
    }
    for(var i = 0; i < jsParts_svr.length; i++) {
        if( parseInt(jsParts_svr[i]) > parseInt(jsParts_clt[i]) ) {
            jsUpdate = true
            break
        }
    }

    outData = {
        'htmlUpdate':htmlUpdate,
        'jsUpdate':jsUpdate
    }

    if( htmlUpdate ) {
        funcs.logMessage(stnId,"checkWebUpdate: ("+socket.id+") Clt Web Needs Updating")
    }

    if( jsUpdate ) {
        funcs.logMessage(stnId,"checkWebUpdate: ("+socket.id+") Clt JS Needs Updating")
    }

    outMsg = funcs.formatMsg("clt","webUpdate",outData)
    socket.send(outMsg)
}

async function getVersions() {
    var dbSql
    var dbRows

    try{
        var admDb = await db.getAdminDb()
        const admT = await admDb.connect()
        try {

            try {
                dbSql = "SELECT * FROM wb_versions" 
                dbRows = await admT.query(dbSql)
                var latestWxData = dbRows.rows[0]

                return latestWxData
                
            } catch(e) {
                funcs.logMessage('ADMIN',"getVersions: ERROR => "+e)
                funcs.logMessage('ADMIN',"getVersions: ERROR => "+maxSql)
            }
            
        } catch (err) {
            funcs.logMessage("ADMIN","getVersions: ERROR =>  "+err)
        } finally {
            admT.release()
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getVersions: ERROR => "+e)
        funcs.logMessage(wxSid,"getVersions: ERROR => "+maxSql)
    }
}

async function getLatestWxData(wxSid,socket) {
    var wxLatestData = {}
    try {
        var stnInfo = await getStationInfo(wxSid)

        var stnDb = await db.getOtherDb(wxSid)
        if( stnDb != -1 ) {
            const dbT = await stnDb.connect()
            try {
                var sensorsSql = "SELECT * FROM wx_data_xref"
                var sensorRows = await dbT.query(sensorsSql)
                var sensorMap = sensorRows.rows[0]

                var latestSql = "SELECT * FROM wx_data WHERE wx_type='all' ORDER BY wx_msec DESC LIMIT 1"
                var latestRows = await dbT.query(latestSql)
                var latestWxData = latestRows.rows[0]

                var uvMax = await getMaxUv('',stnInfo.stnLat,stnInfo.stnLon,stnInfo.stnTz,parseInt(latestWxData.wx_msec),wxSid)

                var max24Data = await getMax24(dbT,wxSid)

                var max60Data = await getWx60MinData(dbT,latestWxData.wx_msec,wxSid)

                var wx10MinData = await getWx10MinData(dbT,latestWxData.wx_msec,wxSid)

                var wx1MinData = await getWx1MinData(dbT,latestWxData.wx_msec,wxSid)

                var wx1MinBaro = await getWx1MinLatestBaro(dbT,wxSid)

                wxLatestData['db_days'] = stnInfo.db_days
                wxLatestData['stnTzOffset'] = stnInfo.stnTz
                wxLatestData['stnVersion'] = stnInfo.wbox_version
                wxLatestData['wxMonthDays'] = max24Data.wxMonthDays
                wxLatestData['wxYearDays'] = max24Data.wxYearDays
                wxLatestData['wx_1_avg'] = wx1MinData
                wxLatestData['wx_10_avg'] = wx10MinData.avg10
                wxLatestData['wx_10_brg'] = wx10MinData.brg10
                wxLatestData['wx_10_brg_max'] = wx10MinData.brg10Max
                wxLatestData['wx_10_brg_min'] = wx10MinData.brg10Min
                wxLatestData['wx_10_max'] = wx10MinData.max10
                wxLatestData['wx_24_max'] = max24Data.wxMax
                wxLatestData['wx_24_max_date'] = max24Data.wxMaxDate
                wxLatestData['wx_24_max_time'] = max24Data.wxMaxTime
                wxLatestData['wx_60_avg'] = max60Data
                wxLatestData['wx_air_in_name'] =sensorMap.air_in_name
                wxLatestData['wx_air_out_name'] =sensorMap.air_out_name
                wxLatestData['wx_baro'] = latestWxData.wx_baro
                wxLatestData['wx_baro_slp'] = latestWxData.wx_baro_slp
                
                wxLatestData['wx_baro_new'] = wx1MinBaro.latestBaro
                wxLatestData['wx_baro_new_msec'] = wx1MinBaro.latestBaroMsec
                wxLatestData['wx_baro_old'] = wx1MinBaro.oldestBaro
                wxLatestData['wx_baro_old_msec'] = wx1MinBaro.oldestBaroMsec

                wxLatestData['wx_dran'] = max24Data.wxRain24
                wxLatestData['wx_humd'] = latestWxData.wx_humid
                wxLatestData['wx_humd_max24'] = max24Data.wxMaxHumd24
                wxLatestData['wx_humd_min24'] = max24Data.wxMinHumd24
                wxLatestData['wx_humdin'] = latestWxData.wx_humid_in
                wxLatestData['wx_latest_date'] = latestWxData.wx_date
                wxLatestData['wx_latest_msec'] = latestWxData.wx_msec
                wxLatestData['wx_latest_time'] = latestWxData.wx_time
                wxLatestData['wx_light_cnt'] = max24Data.wxLightCnt
                wxLatestData['wx_light_dist'] = max24Data.wxLightDist
                wxLatestData['wx_lightning_name'] =sensorMap.lightning_name
                wxLatestData['wx_local_date'] = latestWxData.wx_date
                wxLatestData['wx_local_time'] = latestWxData.wx_time
                wxLatestData['wx_max_uv'] = uvMax
                wxLatestData['wx_mran'] = max24Data.wxMonthRain
                wxLatestData['wx_msec'] = latestWxData.wx_msec
                wxLatestData['wx_rain'] = latestWxData.wx_rain_accum
                wxLatestData['wx_rain24'] = max24Data.wxRain24
                wxLatestData['wx_rain_name'] =sensorMap.rain_name
                wxLatestData['wx_sky_name'] =sensorMap.sky_name
                wxLatestData['wx_stn_lat'] =stnInfo.stnLat
                wxLatestData['wx_stn_lon'] =stnInfo.stnLon
                wxLatestData['wx_stn_name'] = latestWxData.wx_station_name
                wxLatestData['wx_temp'] = latestWxData.wx_temp
                wxLatestData['wx_temp_in'] = latestWxData.wx_temp_in
                wxLatestData['wx_temp_max'] = max24Data.wxTempHi
                wxLatestData['wx_temp_max_time'] = max24Data.wxMaxTempTime
                wxLatestData['wx_temp_min'] = max24Data.wxTempLow
                wxLatestData['wx_temp_min_time'] = max24Data.wxMinTempTime
                wxLatestData['wx_uv'] = latestWxData.wx_uv
                wxLatestData['wx_wbrg'] = latestWxData.wx_bearing
                wxLatestData['wx_wind_name'] =sensorMap.wind_name
                wxLatestData['wx_wspd'] = latestWxData.wx_wind_avg
                wxLatestData['wx_yran'] = max24Data.wxYearRain

                outMsg = funcs.formatMsg("clt","wxLatest",wxLatestData)
                socket.send(outMsg)
            } catch (err) {
                funcs.logMessage(wxSid,"getLatestWxData: ERROR =>  "+err)
            } finally {
                dbT.release()
            }
        } else {
            funcs.logMessage(wxSid,"getLatestWxData: ERROR => Cannot Connect to DB");
        }

    } catch(e) {
        funcs.logMessage(wxSid,"getLatestWxData: ERROR => "+e);
    }
}

async function getLastRW(stnDb,wxSid){
    var maxSql
    var maxRows
    try {
        maxSql = "SELECT * FROM wx_data WHERE wx_type = 'rw' AND wx_public='1' ORDER BY wx_msec DESC LIMIT 1" 
        maxRows = await stnDb.query(maxSql)
        var latestWxData = maxRows.rows[0]

        return latestWxData
        
    } catch(e) {
        funcs.logMessage(wxSid,"getLastRW: ERROR => "+e)
        funcs.logMessage(wxSid,"getLastRW: ERROR => "+maxSql)
    }
}

async function getGraphData(wxData,wxSid,socket) {
    var maxSql
    var maxRows
    try {
        var stnDb = await db.getOtherDb(wxSid)
        if( stnDb != -1 ) {
            const dbT = await stnDb.connect()
            try {
                var latestRw = await getLastRW(dbT,wxSid)
                var localMsec = parseInt(latestRw.wx_msec)
                var timeFirst = localMsec - (parseInt(wxData.mins) * 60)

                maxSql = ""
                var wxDataOut = new Array()
                if( wxData.dataType == "wind" ) {
                    maxSql = "SELECT * FROM wx_data WHERE (wx_type='rw') AND (wx_msec >= "+timeFirst+") ORDER BY wx_msec ASC"
                    maxRows = await dbT.query(maxSql)
    
                    for( var i = 0; i < maxRows.rowCount; i++ ) {
                        var brg = maxRows.rows[i];
    
                        var tData = {}
    
                        tData["wx_date"] = brg.wx_date
                        tData["wx_time"] = brg.wx_time
                        tData["wx_wind_avg"] = brg.wx_wind_avg
                        tData["wx_bearing"] = brg.wx_bearing
    
                        wxDataOut.push(tData)
                    }
                } else {
                    maxSql = "SELECT * FROM wx_data WHERE (wx_type='all') AND (wx_msec >= "+timeFirst+") ORDER BY wx_msec ASC"
                    maxRows = await dbT.query(maxSql)
    
                    for( var i = 0; i < maxRows.rowCount; i++ ) {
                        var brg = maxRows.rows[i];
    
                        var tData = {}
                        
                        tData["wx_date"] = brg.wx_date
                        tData["wx_time"] = brg.wx_time
                        tData["wx_temp"] = brg.wx_temp
                        tData["wx_baro"] = brg.wx_baro
                        tData["wx_humid"] = brg.wx_humid
    
                        wxDataOut.push(tData)
                    }
                } 

                var wxLatestData = {}
                wxLatestData['wx_count'] = maxRows.rowCount 
                wxLatestData['wx_datatype'] = wxData.dataType
                wxLatestData['wx_data'] = wxDataOut
                
                outMsg = funcs.formatMsg("clt","graphData",wxLatestData)
                socket.send(outMsg)
            } catch (err) {
                funcs.logMessage(wxSid,"getGraphData: ERROR =>  "+err)
            } finally {
                dbT.release()
            }
        } else {
            funcs.logMessage(wxSid,"getGraphData: ERROR => Cannot Connect to DB");
        }
        
    } catch(e) {
        funcs.logMessage(wxSid,"getGraphData: ERROR => "+e)
        funcs.logMessage(wxSid,"getGraphData: ERROR => "+maxSql)
    }
}

async function getWxBrg10(wxSid,socket) {
    var maxSql
    var maxRows
    try {
        var stnDb = await db.getOtherDb(wxSid)
        if( stnDb != -1 ) {
            const dbT = await stnDb.connect()
            try {

                var latestRw = await getLastRW(dbT,wxSid)
                
                var localMsec = parseInt(latestRw.wx_msec)
                var nowTime = localMsec - 600

                maxSql = "SELECT wx_bearing as wbrg, wx_wind_avg as wspd FROM wx_data WHERE (wx_bearing != '-999') AND (wx_bearing::real > -999.0) AND (wx_bearing != '-999.0') AND (wx_type = 'rw') AND (wx_public='1') AND (wx_msec >= '"+(nowTime)+"') ORDER BY wx_bearing ASC" 
                maxRows = await dbT.query(maxSql)

                var wxData = "["
                for( var i = 0; i < maxRows.rowCount; i++ ) {
                    var brg = maxRows.rows[i];

                    wxData += '{"wx_bearing'+i+'":"'+brg.wbrg+'","wx_speed'+i+'":"'+brg.wspd+'"}'
                    if( i < (maxRows.rowCount-1) ) {
                        wxData += ","
                    }
                }
                wxData += "]"
                var wxLatestData = {}
                wxLatestData['wx_count'] = maxRows.rowCount 
                wxLatestData['wx_data'] = wxData
                
                outMsg = funcs.formatMsg("clt","wxBrg10",wxLatestData)
                socket.send(outMsg)
            } catch (err) {
                funcs.logMessage(wxSid,"getWxBrg10: ERROR =>  "+err)
            } finally {
                dbT.release()
            }
        } else {
            funcs.logMessage(wxSid,"getWxBrg10: ERROR => Cannot Connect to DB");
        }
        
    } catch(e) {
        funcs.logMessage(wxSid,"getWxBrg10: ERROR => "+e)
        funcs.logMessage(wxSid,"getWxBrg10: ERROR => "+maxSql)
    }
}

async function getWx1MinLatestBaro(stnDb,wxSid) {
    var wx1MinLatestData = {}

    try {
        var sql = "SELECT * FROM wx_data WHERE wx_type = 'all' AND wx_public='1' ORDER BY wx_msec DESC limit 2"  
        var sqlRes = await stnDb.query(sql)
        var latestRow = sqlRes.rows[0]
        var oldestRow = sqlRes.rows[1]

        wx1MinLatestData['latestBaro'] = parseFloat(latestRow.wx_baro)
        wx1MinLatestData['latestBaroMsec'] = parseFloat(latestRow.wx_msec)
        wx1MinLatestData['oldestBaro'] = parseFloat(oldestRow.wx_baro)
        wx1MinLatestData['oldestBaroMsec'] = parseFloat(oldestRow.wx_msec)

    } catch(e) {
        funcs.logMessage(wxSid,"getWx1MinLatestBaro: ERROR => "+e);
    }

    return wx1MinLatestData
}

async function getWx1MinData(stnDb,wxMsec,wxSid) {
    var max1

    try {
        var localMsec = parseInt(wxMsec)
        var nowTime = localMsec - 60

        maxSql = "SELECT MAX(wx_wind_avg) as max1 FROM wx_data WHERE wx_type = 'rw'  AND (wx_wind_avg::real > -999.0) AND wx_wind_avg IS NOT NULL AND wx_public='1' AND wx_msec >= '"+(nowTime)+"'"  
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max1 = maxRow.max1

    } catch(e) {
        funcs.logMessage(wxSid,"getWx1MinData: ERROR => "+e);
    }

    return max1
}

async function getWx10MinData(stnDb,wxMsec,wxSid) {
    var wx10Mindata = {}
    
    try {   
        var localMsec = parseInt(wxMsec)
        var nowTime = localMsec - 600

        maxSql ="SELECT AVG(wx_wind_avg) as wx_wind_avg, MAX(wx_wind_avg) as wx_gust FROM wx_data WHERE (wx_wind_avg IS NOT NULL) AND (wx_wind_avg != '-999') AND (wx_wind_avg != '-999.0') AND (wx_wind_avg::real > -999.0) AND (wx_type = 'rw') AND (wx_public='1') AND (wx_msec >= '"+(nowTime)+"')"
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]
        wx10Mindata['avg10'] = parseFloat(maxRow.wx_wind_avg)
        wx10Mindata['max10'] = parseFloat(maxRow.wx_gust)

        maxSql ="SELECT MIN(wx_bearing) as wx_brg_min, MAX(wx_bearing) as wx_brg_max FROM wx_data WHERE (wx_bearing IS NOT NULL) AND (wx_bearing != '-999') AND (wx_bearing != '-999.0')  AND (wx_bearing::real > -999.0) AND (wx_type = 'rw') AND (wx_public='1') AND (wx_msec >= '"+(nowTime)+"')"
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]
        wx10Mindata['brg10Min'] = parseFloat(maxRow.wx_brg_min)
        wx10Mindata['brg10Max'] = parseFloat(maxRow.wx_brg_max)

        var brg10
        if( (wx10Mindata['brg10Max'] - wx10Mindata['brg10Min']) > 180.0 ) {
            brg10 = ((wx10Mindata['brg10Min']+360.0) + wx10Mindata['brg10Max']) / 2.0
        } else {
            brg10 = (wx10Mindata['brg10Max'] + wx10Mindata['brg10Min']) / 2.0
        }
        if( brg10 > 360.0 ) {
            brg10 = brg10 - 360.0
        }

        wx10Mindata['brg10'] = brg10

    } catch(e) {
        funcs.logMessage(wxSid,"getWx10MinData: ERROR => "+e);
    }

    return wx10Mindata
}

async function getWx60MinData(stnDb,wxMsec,wxSid) {
    var max60

    try {
        var localMsec = parseInt(wxMsec)
        var nowTime = localMsec - 3600

        maxSql = "SELECT MAX(wx_wind_avg) as max60 FROM wx_data WHERE (wx_type = 'rw') AND (wx_wind_avg::real > -999.0) AND (wx_wind_avg IS NOT NULL) AND (wx_public='1') AND (wx_msec >= '"+(nowTime)+"')"  
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max60 = maxRow.max60

    } catch(e) {
        funcs.logMessage(wxSid,"getWx60MinData: ERROR => "+e);
    }

    return max60
}

async function getMax24(stnDb,wxSid) {
    var max24Data = {}

    try {
        var maxSql = "SELECT * from wxMax24RW"
        var maxRows = await stnDb.query(maxSql)
        var maxRow = maxRows.rows[0]

        max24Data['wxMax'] = maxRow.wx_wind_avg
        max24Data['wxMaxTime'] = maxRow.wx_time
        max24Data['wxMaxDate'] = maxRow.wx_date
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: max24 ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: max24 ERROR => "+maxSql);
    }

    try {
        maxSql = `SELECT MIN(wx_humid) AS minhumd, MAX(wx_humid) AS maxhumd FROM wx_data WHERE (wx_humid != '-999') AND (wx_humid != '-999.0') AND (wx_humid::real > -999.0) AND (wx_humid::real < 255.0)
        AND (wx_date = (SELECT wx_date FROM wx_data ORDER BY wx_msec DESC limit 1) )`
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max24Data['wxMaxHumd24'] = maxRow.maxhumd
        max24Data['wxMinHumd24'] = maxRow.minhumd
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: min-maxHumd ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: min-maxHumd ERROR => "+maxSql);
    }

    try {
        maxSql = `SELECT wx_temp, wx_time FROM wx_data WHERE (wx_temp != '-999') AND (wx_temp != '-999.0') AND ( wx_temp::REAL < 100.0) AND ( wx_temp::REAL > -999)
            AND (wx_date = (SELECT wx_date FROM wx_data ORDER BY wx_msec DESC limit 1) ) ORDER BY wx_temp ASC LIMIT 1`
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max24Data['wxTempLow'] = maxRow.wx_temp
        max24Data['wxMinTempTime'] = maxRow.wx_time
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: minTemp ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: minTemp ERROR => "+maxSql);
    }

    try {
        maxSql = `SELECT wx_temp, wx_time FROM wx_data WHERE (wx_temp != '-999') AND (wx_temp != '-999.0') AND ( wx_temp::REAL < 100.0) AND ( wx_temp::REAL > -999)
            AND (wx_date = (SELECT wx_date FROM wx_data ORDER BY wx_msec DESC limit 1) ) ORDER BY wx_temp DESC LIMIT 1`
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max24Data['wxTempHi'] = maxRow.wx_temp
        max24Data['wxMaxTempTime'] = maxRow.wx_time
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: maxTemp ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: maxTemp ERROR => "+maxSql);
    }
    
    try {
        maxSql = `SELECT SUM(wx_light_count::integer) AS lightcnt FROM wx_data WHERE (wx_light_count != '-999') AND (wx_light_count != '-999.0') AND (wx_light_count::integer > -999)
            AND (wx_date = (SELECT wx_date FROM wx_data ORDER BY wx_msec DESC limit 1) )`
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max24Data['wxLightCnt'] = maxRow.lightcnt
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: lightcnt ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: lightcnt ERROR => "+maxSql);
    }
    
    try {
        maxSql = `SELECT AVG(wx_light_dist) AS lightdist FROM wx_data WHERE (wx_light_dist IS NOT NULL) AND (wx_light_dist != '-999') AND (wx_light_dist != '-999.0') AND (wx_light_dist::real > -999.0)
            AND (wx_date = (SELECT wx_date FROM wx_data ORDER BY wx_msec DESC limit 1) ) AND wx_light_count>0`
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max24Data['wxLightDist'] = maxRow.lightdist
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: lightdist ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: lightdist ERROR => "+maxSql);
    }

    try {
        // maxSql = `SELECT SUM(wx_rain_accum) as accumRain FROM 
        //     (SELECT * FROM wx_data where (wx_type ='all') and (wx_rain_accum != '-999') AND (wx_rain_accum::real > -0.001) 
        //         AND (wx_date = (SELECT wx_date FROM wx_data ORDER BY wx_msec DESC limit 1) ) 
        //     order by wx_msec desc) as data24`
        maxSql = `SELECT wx_rain_accum_day as accumRain FROM wx_data 
            WHERE (wx_type ='all') 
            ORDER BY wx_msec DESC limit 1`
        maxRows = await stnDb.query(maxSql)
        maxRow = maxRows.rows[0]

        max24Data['wxRain24'] = maxRow.accumrain
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: rain24 ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: rain24 ERROR => "+maxSql);
    }

    try {
        dateSql = "SELECT CAST(wx_date AS text) FROM wx_data ORDER BY wx_msec DESC limit 1"
        dateRows = await stnDb.query(dateSql)
        dateRow = dateRows.rows[0]

        var rainDateM = dateRow.wx_date
        rainDateM = rainDateM.substring(0,7)

        datesSql = `SELECT CAST(wx_date AS text) FROM wx_data WHERE (wx_type ='all') AND (CAST(wx_date AS text) LIKE '%`+rainDateM+`-%') GROUP BY wx_date ORDER BY wx_date ASC`
        datesRows = await stnDb.query(datesSql)
        datesCnt = datesRows.rowCount

        max24Data['wxMonthDays'] = datesCnt

        accumM = 0;
        dateRainSql = `SELECT SUM(rain_accum) as accum FROM 
        (SELECT wx_date,MAX(wx_rain_accum_day) AS rain_accum FROM 
            (SELECT * FROM wx_data 
                WHERE (CAST(wx_date AS text) LIKE '%`+rainDateM+`%') AND (wx_type='all') 
                AND (wx_rain_accum_day IS NOT NULL) AND (wx_rain_accum_day != '-999') AND (wx_rain_accum_day != '-999.0') AND (wx_rain_accum_day::real > -999.0) 
                ORDER BY wx_msec DESC) as foo 
            GROUP BY wx_date) as foo2;`
        dateRainRows = await stnDb.query(dateRainSql)
        dateRainRow = dateRainRows.rows[0]
        
        max24Data['wxMonthRain'] = dateRainRow.accum

    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: monthRain ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: monthRain ERROR => "+maxSql);
    }

    try {        
        dateSql = "SELECT CAST(wx_date AS text) FROM wx_data ORDER BY wx_msec DESC limit 1"
        dateRows = await stnDb.query(dateSql)
        dateRow = dateRows.rows[0]

        var rainDateY = dateRow.wx_date
        rainDateY = rainDateY.substring(0,4)

        datesSql = `SELECT CAST(wx_date AS text) FROM wx_data WHERE (wx_type ='all') AND (CAST(wx_date AS text) LIKE '%`+rainDateY+`%') GROUP BY wx_date ORDER BY wx_date ASC`
        datesRows = await stnDb.query(datesSql)
        datesCnt = datesRows.rowCount

        max24Data['wxYearDays'] = datesCnt

        accumY = 0;
        dateRainSql = `SELECT SUM(rain_accum) as accum FROM 
        (SELECT wx_date,MAX(wx_rain_accum_day) AS rain_accum FROM 
            (SELECT * FROM wx_data 
                WHERE (CAST(wx_date AS text) LIKE '%`+rainDateY+`-%') AND (wx_type='all') 
                AND (wx_rain_accum_day IS NOT NULL) AND (wx_rain_accum_day != '-999') AND (wx_rain_accum_day != '-999.0') AND (wx_rain_accum_day::real > -999.0) 
                ORDER BY wx_msec DESC) as foo 
            GROUP BY wx_date) as foo2;`
        dateRainRows = await stnDb.query(dateRainSql)
        dateRainRow = dateRainRows.rows[0]
        
        max24Data['wxYearRain'] = dateRainRow.accum
    } catch(e) {
        funcs.logMessage(wxSid,"getMax24: yearRain ERROR => "+e);
        funcs.logMessage(wxSid,"getMax24: yearRain ERROR => "+maxSql);
    }

    return max24Data
}

async function getMonthUvMax(uvDb,wxLat,wxLon,monthS,monthN) {
    var uv = new Array(11)
    for( var i = 0; i < uv.length; i++) {
        uv[i] = new Array(11)
    }

    var uv1, uv2, uv3, uv4, uv5, uv6, uv7, uv8, uv9

    var lat = Math.floor(wxLat)
    var lon = Math.floor(wxLon)

    //get row 0
    var getSql = "SELECT "
    if( lon < 0 ) {
        getSql = getSql+"lon"+Math.abs(lon-1)+"w as uv1, "
        getSql = getSql+"lon"+Math.abs(lon)+"w as uv2, "
        getSql = getSql+"lon"+Math.abs(lon+1)+"w as uv3"
    } else {
        getSql = getSql+"lon"+Math.abs(lon-1)+"e as uv1, "
        getSql = getSql+"lon"+Math.abs(lon)+"e as uv2, "
        getSql = getSql+"lon"+Math.abs(lon+1)+"e as uv3"
    }
    getSql = getSql+" FROM wx_uv_index_"+monthN+"_"+monthS+" WHERE lat='"+(lat-1)+"' LIMIT 1"

    var row0Data = await uvDb.query(getSql)
    var row = row0Data.rows[0]
    uv1 = parseFloat(row.uv1)
    uv2 = parseFloat(row.uv2)
    uv3 = parseFloat(row.uv3)
    
    uv[0][0] = uv1
    uv[0][5] = uv2
    uv[0][10] = uv3

    //get row 5
    var getSql = "SELECT "
    if( lon < 0 ) {
        getSql = getSql+"lon"+Math.abs(lon-1)+"w as uv4, "
        getSql = getSql+"lon"+Math.abs(lon)+"w as uv5, "
        getSql = getSql+"lon"+Math.abs(lon+1)+"w as uv6"
    } else {
        getSql = getSql+"lon"+Math.abs(lon-1)+"e as uv4, "
        getSql = getSql+"lon"+Math.abs(lon)+"e as uv5, "
        getSql = getSql+"lon"+Math.abs(lon+1)+"e as uv6"
    }
    getSql = getSql+" FROM wx_uv_index_"+monthN+"_"+monthS+" WHERE lat='"+lat+"' LIMIT 1"

    var row5Data = await uvDb.query(getSql)
    var row = row5Data.rows[0]
    uv4 = parseFloat(row.uv4)
    uv5 = parseFloat(row.uv5)
    uv6 = parseFloat(row.uv6)

    uv[5][0] = uv4
    uv[5][5] = uv5
    uv[5][10] = uv6

    //get row 10
    var getSql = "SELECT "
    if( lon < 0 ) {
        getSql = getSql+"lon"+Math.abs(lon-1)+"w as uv7, "
        getSql = getSql+"lon"+Math.abs(lon)+"w as uv8, "
        getSql = getSql+"lon"+Math.abs(lon+1)+"w as uv9"
    } else {
        getSql = getSql+"lon"+Math.abs(lon-1)+"e as uv7, "
        getSql = getSql+"lon"+Math.abs(lon)+"e as uv8, "
        getSql = getSql+"lon"+Math.abs(lon+1)+"e as uv9"
    }
    getSql = getSql+" FROM wx_uv_index_"+monthN+"_"+monthS+" WHERE lat='"+(lat+1)+"' LIMIT 1"

    var row10Data = await uvDb.query(getSql)
    var row = row10Data.rows[0]
    uv7 = parseFloat(row.uv7)
    uv8 = parseFloat(row.uv8)
    uv9 = parseFloat(row.uv9)

    uv[10][0] = uv7
    uv[10][5] = uv8
    uv[10][10] = uv9

    //fill rows 0, 5, 10
    var uv0Diff = Math.abs(uv2 - uv1)/5
    var uv5Diff = Math.abs(uv5 - uv4)/5
    var uv10Diff = Math.abs(uv8 - uv7)/5

    for( var i = 1; i < 5; i++ ) {
        uv[0][i]  = uv[0][i-1]  + uv0Diff
        uv[5][i]  = uv[5][i-1]  + uv5Diff
        uv[10][i] = uv[10][i-1] + uv10Diff
    }

    uv0Diff = Math.abs(uv3 - uv2)/5
    uv5Diff = Math.abs(uv6 - uv5)/5
    uv10Diff = Math.abs(uv9 - uv8)/5

    for( var i = 6; i < 10; i++ ) {
        uv[0][i]  = uv[0][i-1]  + uv0Diff
        uv[5][i]  = uv[5][i-1]  + uv5Diff
        uv[10][i] = uv[10][i-1] + uv10Diff
    }

    //fill cols 0, 5, 10
    uv0Diff = Math.abs(uv4 - uv1)/5
    uv5Diff = Math.abs(uv5 - uv2)/5
    uv10Diff = Math.abs(uv6 - uv3)/5

    for( var i = 1; i < 5; i++ ) {
        uv[i][0]  = uv[i-1][0]  + uv0Diff
        uv[i][5]  = uv[i-1][5]  + uv5Diff
        uv[i][10] = uv[i-1][10] + uv10Diff
    }

    uv0Diff = Math.abs(uv7 - uv4)/5
    uv5Diff = Math.abs(uv8 - uv5)/5
    uv10Diff = Math.abs(uv9 - uv6)/5

    for( var i = 6; i < 10; i++ ) {
        uv[i][0]  = uv[i-1][0]  + uv0Diff
        uv[i][5]  = uv[i-1][5]  + uv5Diff
        uv[i][10] = uv[i-1][10] + uv10Diff
    }

    //fill in remaining holes
    for( var j = 1; j < 5; j++ ) {
        var uvDiff = Math.abs(uv[j][5] - uv[j][0])/5
        for( var i = 1; i < 5; i++ ) {
            uv[j][i]  = uv[j][i-1]  + uvDiff
        }
        var uvDiff = Math.abs(uv[j][10] - uv[j][5])/5
        for( var i = 6; i < 10; i++ ) {
            uv[j][i]  = uv[j][i-1]  + uvDiff
        }
    }   
    for( var j = 6; j < 10; j++ ) {
        var uvDiff = Math.abs(uv[j][5] - uv[j][0])/5
        for( var i = 1; i < 5; i++ ) {
            uv[j][i]  = uv[j][i-1]  + uvDiff
        }
        var uvDiff = Math.abs(uv[j][10] - uv[j][5])/5
        for( var i = 6; i < 10; i++ ) {
            uv[j][i]  = uv[j][i-1]  + uvDiff
        }
    }

    return uv
}

async function getMaxUv(response,wxLat,wxLon,stnTimezone, wxMsec) {
    try{
        var uvDb = await db.getUvDb()
        
        if( uvDb != -1 ) {
            const dbT = await uvDb.connect()
            try {
                const date = new Date(); 
                const monthS = date.toLocaleString('default', { month: 'short' });  
                const monthN = date.toLocaleString('default', { month: '2-digit' });  

                var uvThisMonth = await getMonthUvMax(dbT,wxLat,wxLon,monthS,monthN)

                var lastMonth = new Date();
                lastMonth.setDate(date.getDate());
                lastMonth.setMonth(lastMonth.getMonth()-1);
                
                const monthLS = lastMonth.toLocaleString('default', { month: 'short' });  
                const monthLN = lastMonth.toLocaleString('default', { month: '2-digit' }); 

                var uvLastMonth = await getMonthUvMax(dbT,wxLat,wxLon,monthLS,monthLN)

                //find closest to input lat/lon
                var lat10th = wxLat - (Math.floor(wxLat))
                lat10th = Math.round(10 * lat10th)
                lat10th = 2 * (Math.round(lat10th / 2))

                var lon10th = wxLon - (Math.floor(wxLon))
                lon10th = Math.round(10 * lon10th)
                lon10th = 2 * (Math.round(lon10th / 2))

                var myUvThisMonth = Math.round(uvThisMonth[lat10th][lon10th] * 10) / 10
                var myUvLastMonth = Math.round(uvLastMonth[lat10th][lon10th] * 10) / 10

                var now = new Date()
                var daysThisMonth = new Date(now.getFullYear(), now.getMonth()+1, 0).getDate()
                var currentDay = now.getDate()

                var uvDiff = (myUvThisMonth - myUvLastMonth) / daysThisMonth

                var currentDayMax = Math.round((myUvLastMonth + (uvDiff * currentDay)) * 10) / 10

                var stationTzOffset = parseInt(stnTimezone);

                if( wxMsec == 0 ) {
                    wxMsec = Math.round(date.getTime()/1000)
                }

                var luxDate = luxon.DateTime.fromSeconds(wxMsec)
                if( stationTzOffset >= 0 ) {
                    luxDate = luxDate.setZone("UTC+"+stationTzOffset)
                } else {
                    luxDate = luxDate.setZone("UTC"+stationTzOffset)
                }	

                var sunR = MeeusSunMoon.sunrise(luxDate,parseFloat(wxLat),parseFloat(wxLon))
                var sunS = MeeusSunMoon.sunset(luxDate,parseFloat(wxLat),parseFloat(wxLon))
                var solN = MeeusSunMoon.solarNoon(luxDate,parseFloat(wxLon))

                var sunriseHours = sunR.hour
                var sunriseMinutes = sunR.minute
                var solNoonHours = solN.hour
                var solNoonMinutes = solN.minute
                var sunsetHours = sunS.hour
                var sunsetMinutes = sunS.minute

                var nowHours = now.getHours()
                var nowMinutes = now.getMinutes()

                var sunRHours = sunriseHours + (sunriseMinutes/60)
                var sunSHours = sunsetHours + (sunsetMinutes/60)
                var solNHours = solNoonHours + (solNoonMinutes/60)
                var nowUvHours = nowHours + (nowMinutes/60)

                var myUv = 0
                var hrsRatio = 1
                if( nowUvHours < solNHours ) {
                    hrsRatio = (nowUvHours - (sunRHours-2)) / (solNHours - (sunRHours-2))
                } else if( nowUvHours > solNHours ) {
                    hrsRatio = ((sunSHours+2) - nowUvHours) / ((sunSHours+2) - solNHours)
                }
                
                myUv = Math.round((currentDayMax * hrsRatio) * 10) / 10

                if( response != '' ) {
                    returnMaxUv(response,myUv,wxLat,wxLon)
                } else {
                    return myUv
                }
            } catch (err) {
                funcs.logMessage(wxSid,"getMaxUv: ERROR =>  "+err)
            } finally {
                dbT.release()
            }
        } else {
            funcs.logMessage(wxSid,"getMaxUv: ERROR => Cannot Connect to DB");
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"getMaxUv: ERROR => "+e)
        funcs.logMessage(wxSid,"getMaxUv: ERROR => "+maxSql)
    }
}

function returnMaxUv(response,myUv,wxLat,wxLon) {
    funcs.logMessage('ADMIN',"returnMaxUv: ("+wxLat+","+wxLon+") => "+myUv)
    response.status(200).json(myUv)
}

async function insertWxData(wxData) {
    try {
        var stnDb = await db.getOtherDb(wxData.wx_sid)
        if( stnDb != -1 ) {
            try {
                const dbT = await stnDb.connect()
                try {
                    var insertWxBase = "INSERT INTO wx_data "
                    var insertWxFields = ""
                    var insertWxData = ""

                    if( wxData.wx_data == "all" ) {
                        insertWxFields = "(wx_sid,wx_wind_lull,wx_wind_avg,wx_wind_gust,wx_bearing,wx_baro,"
                        insertWxFields += "wx_rain_accum,wx_rain_accum_day,wx_temp,wx_humid,wx_uv,"
                        insertWxFields += "wx_solar_rad,wx_illum,wx_light_count,wx_light_dist,wx_msec,wx_date_utc,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_type,wx_public,wx_humid_in,wx_temp_in,wx_baro_slp)"

                        insertWxData = "('"+(wxData.wx_sid)+"','"+(wxData.wx_wind_lull)+"','"+(wxData.wx_wind_avg)+"','"+(wxData.wx_wind_gust)+"','"+(wxData.wx_bearing)+"','"+(wxData.wx_baro)+"',"
                        insertWxData += "'"+(wxData.wx_rain_accum)+"','"+(wxData.wx_rain_accum_day)+"','"+(wxData.wx_temp)+"','"+(wxData.wx_humid)+"','"+(wxData.wx_uv)+"','"+(wxData.wx_sol_rad)+"',"
                        insertWxData += "'"+(wxData.wx_illum)+"','"+(wxData.wx_light_count)+"','"+(wxData.wx_light_dist)+"','"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"','"+(wxData.wx_local_time)+"','"+(wxData.wx_station_name)+"','"+(wxData.wx_data)+"','"+(wxData.wx_public)+"',"
                        insertWxData += "'"+(wxData.wx_humid_in)+"','"+(wxData.wx_temp_in)+"','"+(wxData.wx_baro_slp)+"')"
                    } else if( wxData.wx_data == "lightning" ) {
                        insertWxFields = "(wx_sid,wx_light_count,wx_light_dist"
                        insertWxFields += "wx_msec,wx_date_utc,wx_sensor_id,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_type,wx_public)"

                        insertWxData = "('"+(wxData.wx_sid)+"','"+(wxData.wx_light_count)+"','"+(wxData.wx_light_dist)+"','"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"',"
                        insertWxData += "'"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"','"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"','"+(wxData.wx_local_time)+"','"+(wxData.wx_station_name)+"','"+(wxData.wx_data)+"','"+(wxData.wx_public)+"')"

                    } else if( wxData.wx_data == "wind" ) {
                        insertWxFields = "(wx_sid,wx_wind_lull,wx_wind_avg,wx_wind_gust,wx_bearing,"
                        insertWxFields += "wx_msec,wx_date_utc,wx_sensor_id,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_type,wx_public)"

                        insertWxData = "('"+(wxData.wx_sid)+"','"+(wxData.wx_wind_lull)+"','"+(wxData.wx_wind_avg)+"','"+(wxData.wx_wind_gust)+"','"+(wxData.wx_bearing)+"','"
                        insertWxData += "'"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"',"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"','"+(wxData.wx_local_time)+"','"+(wxData.wx_station_name)+"','"+(wxData.wx_data)+"','"+(wxData.wx_public)+"')"

                    } else if( wxData.wx_data == "rain" ) {
                        insertWxFields = "(wx_sid,wx_rain_accum,wx_rain_accum_day,"
                        insertWxFields += "wx_msec,wx_date_utc,wx_sensor_id,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_type,wx_public)"

                        insertWxData = "('"+(wxData.wx_sid)+"','"+(wxData.wx_rain_accum)+"','"+(wxData.wx_rain_accum_day)+"',"
                        insertWxData += "'"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"',"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"','"+(wxData.wx_local_time)+"','"+(wxData.wx_station_name)+"','"+(wxData.wx_data)+"','"+(wxData.wx_public)+"')"

                    } else if( wxData.wx_data == "sky" ) {
                        insertWxFields = "(wx_sid,wx_uv,wx_solar_rad,wx_illum,"
                        insertWxFields += "wx_msec,wx_date_utc,wx_sensor_id,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_type,wx_public)"

                        insertWxData = "('"+(wxData.wx_sid)+"','"+(wxData.wx_uv)+"','"+(wxData.wx_sol_rad)+"','"+(wxData.wx_illum)+"',"
                        insertWxData += "'"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"',"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"','"+(wxData.wx_local_time)+"','"+(wxData.wx_station_name)+"','"+(wxData.wx_data)+"','"+(wxData.wx_public)+"')"

                    } else if( (wxData.wx_data == "air") || (wxData.wx_data == "air") ||  (wxData.wx_data == "air") ) {
                        insertWxFields = "(wx_sid,wx_baro,,wx_temp,wx_humid,"
                        insertWxFields += "wx_msec,wx_date_utc,wx_sensor_id,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_type,wx_public)"

                        insertWxData = "('"+(wxData.wx_sid)+"','"+(wxData.wx_baro)+"','"+(wxData.wx_temp)+"','"+(wxData.wx_humid)+"',"
                        insertWxData += "'"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"',"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"','"+(wxData.wx_local_time)+"','"+(wxData.wx_station_name)+"','"+(wxData.wx_data)+"','"+(wxData.wx_public)+"')"

                    } else if( wxData.wx_data == "rw" ) {
                        insertWxFields = "(wx_sid,wx_wind_avg,wx_bearing,"
                        insertWxFields += "wx_msec,wx_date_utc,wx_sensor_id,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_type,wx_public)"

                        insertWxData = "('"+(wxData.wx_sid)+"','"+(wxData.wx_wind_avg)+"','"+(wxData.wx_bearing)+"',"
                        insertWxData += "'"+(wxData.wx_msec)+"','"+(wxData.wx_dateUTC)+"','"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"','"+(wxData.wx_local_time)+"','"+(wxData.wx_station_name)+"','"+(wxData.wx_data)+"','"+(wxData.wx_public)+"')"
                    } else if( wxData.wx_data == "evt_lightning" ) {
                        funcs.logMessage(wxData.wx_sid,"insertWxData: "+wxData.wx_data)
                        insertWxFields = "(wx_sid,wx_msec,wx_date_utc,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_sensor_id,wx_type,wx_public,wx_light_dist)"

                        insertWxData = "('"+(wxData.wx_sid)+"',"
                        insertWxData += "'"+(wxData.wx_msec)+"',"
                        insertWxData += "'"+(wxData.wx_dateUTC)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"',"
                        insertWxData += "'"+(wxData.wx_local_time)+"',"
                        insertWxData += "'"+(wxData.wx_station_name)+"',"
                        insertWxData += "'"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_data)+"',"
                        insertWxData += "'"+(wxData.wx_public)+"',"
                        insertWxData += "'"+(wxData.wx_light_dist)+"')"

                    } else if( wxData.wx_data == "evt_precip" ) {
                        funcs.logMessage(wxData.wx_sid,"insertWxData: "+wxData.wx_data)
                        insertWxFields = "(wx_sid,wx_msec,wx_date_utc,"
                        insertWxFields += "wx_date,wx_time,wx_station_name,wx_sensor_id,wx_type,wx_public)"

                        insertWxData = "('"+(wxData.wx_sid)+"',"
                        insertWxData += "'"+(wxData.wx_msec)+"',"
                        insertWxData += "'"+(wxData.wx_dateUTC)+"',"
                        insertWxData += "'"+(wxData.wx_local_date)+"',"
                        insertWxData += "'"+(wxData.wx_local_time)+"',"
                        insertWxData += "'"+(wxData.wx_station_name)+"',"
                        insertWxData += "'"+(wxData.wx_sensor_id)+"',"
                        insertWxData += "'"+(wxData.wx_data)+"',"
                        insertWxData += "'"+(wxData.wx_public)+"')"
                    }

                    if( (insertWxFields != "") && (insertWxData != "") ) {
                        const searchRegExp = /null/gi;
                        const replaceWith = '-999'
                        insertWxData = insertWxData.replace(searchRegExp,replaceWith)

                        var insertWx = insertWxBase+insertWxFields+" VALUES "+insertWxData
                        try {
                            await dbT.query(insertWx)
                            await updateStnMsec(wxData.wx_sid)

                            var stnFile = '/home/beenth12/www/ws/wxbox/node/stn_data/'+wxData.wx_sid+'_'+wxData.wx_data+'.json';

                            if (fs.existsSync(stnFile)) {fs.unlinkSync(stnFile)};
                            fs.writeFileSync(stnFile,JSON.stringify(wxData), { flag: 'a+' })
                        } catch(e) {
                            funcs.logMessage(wxData.wx_sid,"insertWxData: ERROR1 => "+e)
                            funcs.logMessage(wxData.wx_sid,"insertWxData: ERROR1 => "+insertWx)
                            funcs.logMessage(wxData.wx_sid,"insertWxData: ERROR2 => "+JSON.stringify(wxData))
                        }
                    }
                } catch (err) {
                    funcs.logMessage(wxData.wx_sid,"insertWxData: ERROR =>  "+err)
                } finally {
                    dbT.release()
                }

                await admFuncs.setStationData(wxData.wx_sid,"1") 
            } catch(e) {
                funcs.logMessage(wxData.wx_sid,"insertWxData: ERROR2 => "+e)
                funcs.logMessage(wxData.wx_sid,"insertWxData: ERROR2 => "+JSON.stringify(wxData))
            }
        } else {
            funcs.logMessage(wxSid,"insertWxData: ERROR => Cannot Connect to DB");
        }        
    } catch(e) {
        funcs.logMessage(wxSid,"insertWxData: ERROR => "+e)
        funcs.logMessage(wxSid,"insertWxData: ERROR => "+maxSql)
    }
}

module.exports = {
    getAllStations,
    getStationBySid,
    checkStnSubscription,
    checkStnUpdate,
    insertWxData,
    getLatestWxData,
    getStationInfo,
    getMaxUv,
    getWebUv,
    updateStnXref,
    updateStnInfo,
    getWxBrg10,
    getGraphData,
    checkWebUpdate,
    updateStnMsec,
    getClosestStations_NWS,
    getClosestStations_NOAA,
    getNoaaObs,
    getNwsObs,
    getNwsAlerts,
    checkStnUpdateAll,
}