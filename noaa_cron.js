var Pool = require('pg-pool')

const fetch = require('sync-fetch')

var noaaApi = "EweXvCWyrpHcegveVsOVWROWRgmRfaOF"

const noaaNwsDb = new Pool({
    user: 'root',
    host: '127.0.0.1',
    database: 'stn_other',
    password: 'SailView2627!',
    port: 5432,
    max: 500,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 120000,
    maxUses: 7500,
})

async function getNoaaStations(startTime) {
  try {
    var tDate = new Date(1970, 0, 1)
    tDate.setSeconds(startTime)
    var timeString = tDate.toTimeString()
    console.log("NOAA: Stn Startup => "+timeString)

    // var noaaNwsDb = db.getNoaaNwsDb()
    //const noaaNwsT = noaaNwsDb.connect()
    //var noaaNwsT = await noaaNwsDb.connect()

    //var apiQry = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
    var apiQry = "https://www.ncdc.noaa.gov/cdo-web/api/v2/"

    var noaaResp = fetch(apiQry)
    var noaaFile = noaaResp.text()

    var noaaFileContent = noaaFile.split("\n")
    console.log("NOAA: Stn Total => "+noaaFileContent.length)

    var index = 0

    noaaFileContent.forEach(async function(stnLine) {
        if( stnLine.substring(0,1) != "#" ) {
            if( stnLine.length > 0 ) {
                var stnData = stnLine.split("|")
                if( stnData != '' ) {
                    if( Array.isArray(stnData) ) {
                        var stnId = stnData[0].trim().toUpperCase()
                        var stnType = "NOAA"
                        var stnName = stnData[2].trim()
                        if( stnName == "" ) stnName = stnId
                        var stnLoc = stnData[6].trim()
                        
                        var coords = stnLoc.split("(")
                        var stnCoords
                        if( coords[0].indexOf("N") != -1 ) {
                            stnCoords = coords[0].split(" N ")
                        } else if( coords[0].indexOf("S") != -1 ) {
                            stnCoords = coords[0].split(" S ")
                            stnCoords[0] = parseFloat(stnCoords[0]) * -1.0
                        }

                        if( stnCoords[1].indexOf("E") != -1 ) {
                            stnCoords[1] = stnCoords[1].substring(0,stnCoords[1].length - 3)
                        } else if( stnCoords[1].indexOf("W") != -1 ) {
                            stnCoords[1] = stnCoords[1].substring(0,stnCoords[1].length - 3)
                            stnCoords[1] = parseFloat(stnCoords[1]) * -1.0
                        }

                        var sql_base = "insert into wx_stn (stn_id,stn_lat,stn_lon,stn_name,stn_type) values "

                        var sql_data = "("
                        sql_data = sql_data+"'"+stnId+"',"
                        sql_data = sql_data+"'"+stnCoords[0]+"',"
                        sql_data = sql_data+"'"+stnCoords[1]+"',"
                        sql_data = sql_data+"'"+stnName+"',"
                        sql_data = sql_data+"'"+stnType+"'"
                        sql_data = sql_data+")"
                        
                        var sql_end = " ON CONFLICT (stn_id) DO NOTHING;"
                            
                        var sql = sql_base+sql_data+sql_end

                        var noaaNwsT = await noaaNwsDb.connect()
                        try {
                            await noaaNwsT.query(sql)
                        } catch(e) {
                            console.log('NOAA: Stn Error1 ('+index+') => ', sql)
                            console.log('NOAA: Stn Error1 ('+index+') => ', e)
                        } finally {
                            await noaaNwsT.release()
                        }
                    }
                }
            }
        }
        index++
    })
    console.log("NOAA: Exiting Stn...")
  }
  catch(e) {
    console.log('NOAA: Stn Error => ', e)
  }
}

function checkMM(inData) {
  inData = inData.trim()
  // var pluspos = inData.indexOf("+")
  // if( pluspos != -1 ) {
  //   inData = inData.substring(pluspos+1)
  // }

  if( inData.indexOf("MM") != -1 ) {
    inData = "-999"
  }

  return inData
}

async function getNoaaObs(startTime) {
  try {
    var tDate = new Date(1970, 0, 1)
    tDate.setSeconds(startTime)
    var timeString = tDate.toTimeString()
    console.log("NOAA: Obs Startup => "+timeString)

    var tzOffset = new Date().getTimezoneOffset()
    console.log("NOAA: Local TZ Offset => "+(tzOffset * 60))

    var nowMsec = Math.floor(Date.now() / 1000) - (tzOffset * 60)
    var nowOldMsec = nowMsec  - (60 * 60 * 24)

    console.log("NOAA: Deleting older records => "+nowOldMsec)
    var sql = "DELETE FROM wx_obs WHERE stn_msec<'" + nowOldMsec + "';"
    var noaaNwsT = await noaaNwsDb.connect()
    noaaNwsT.query(sql) 

    var index = 0

    var apiQry = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
    
    console.log("NOAA: Latest Url => "+apiQry)

    var noaaResp = fetch(apiQry)
    var noaaFile = noaaResp.text()
    var noaaFileContent = noaaFile.split("\n")
    
    console.log("NOAA: Total Obs => "+noaaFileContent.length)
    
    noaaFileContent.forEach(async function(stnLine) {
        if( stnLine.substring(0,1) != "#" ) {
            var stationID = stnLine.substring(0,5).trim().toUpperCase()
            if( stationID != "" ) {

                // var lat = parseFloat(checkMM(stnLine.substring(6,7).trim()))
                // var lon = parseFloat(checkMM(stnLine.substring(14,8).trim()))

                var wbrgT = stnLine.substring(40,43)
                wbrgT = checkMM(wbrgT)
                var wbrg = parseInt(wbrgT)

                var wspdT = stnLine.substring(44,49)
                wspdT = checkMM(wspdT)
                var wspd = parseFloat(wspdT)

                var wgstT = stnLine.substring(50,55)
                wgstT = checkMM(wgstT)
                var wgst = parseFloat(wgstT)

                var baroT = stnLine.substring(74,80)
                baroT = checkMM(baroT)
                var baro = parseFloat(baroT)

                var tempT = stnLine.substring(87,92)
                tempT = checkMM(tempT)
                var temp = parseFloat(tempT)

                var dewpT = stnLine.substring(100,104)
                dewpT = checkMM(dewpT)
                var dewp = parseFloat(dewpT)

                if( (wbrg != -999) &&
                    (wspd != -999.0) &&
                    (wgst != -999.0) &&
                    (baro != -999.0) &&
                    (temp != -999.0) &&
                    (dewp != -999.0) 
                    ) {

                    var year = stnLine.substring(23,27).trim()
                    var month = stnLine.substring(28,30).trim()
                    var day = stnLine.substring(31,33).trim()
                    var hour = stnLine.substring(34,36).trim()
                    var minute = stnLine.substring(37,39).trim()

                    var stnDate = new Date()
                    stnDate.setFullYear(year)
                    stnDate.setMonth(month-1)
                    stnDate.setDate(day)
                    stnDate.setHours(hour)
                    stnDate.setMinutes(minute)
                    stnDate.setSeconds("00")
                    var msec = Math.round(stnDate.getTime()/1000)

                    msec = msec - (tzOffset * 60)

                    var sql_base = "insert into wx_obs (stn_id,stn_msec,stn_wbrg,stn_wspd,stn_wgst,stn_baro,stn_temp,stn_dewp) values "

                    var sql_data = "("
                    sql_data = sql_data+"'" + stationID + "','" + msec + "','"  + wbrg + "'," 
                    sql_data = sql_data+"'" + wspd + "','" + wgst + "','" + baro + "','" + temp + "',"
                    sql_data = sql_data+"'" + dewp + "')"

                    var sql_end = " on conflict (stn_id) do update set stn_msec='"+msec+"',stn_wbrg='"+wbrg+"',stn_wspd='"+wspd+"'"
                    sql_end = sql_end+",stn_wgst='"+wgst+"',stn_baro='"+baro+"',stn_temp='"+temp+"'"
                    sql_end = sql_end+",stn_dewp='"+dewp+"';"

                    var sql = sql_base+sql_data+sql_end
                    var noaaNwsT2 = await noaaNwsDb.connect()
                    try {
                        await noaaNwsT2.query(sql)
                    } catch(e) {
                        console.log('NOAA: Obs Error1 ('+index+') => ', sql)
                        console.log('NOAA: Obs Error1 ('+index+') => ', e)
                    } finally {
                        await noaaNwsT2.release()
                    } 

                    sql_data = ""
                    sql_end = ""
                }
            }
        }
        index++
    })
  } catch(e) {
    console.log('NOAA: Obs Error2 => ', e)
  } finally {
    console.log("NOAA: Exiting Obs...")
    await noaaNwsT.release()
  }
}

var myArgs = process.argv.slice(2)

var current_ts = Math.floor(Date.now() / 1000) 

switch (myArgs[0]) {
    case 'stn':
        console.log('NOAA: Process Stn')
        getNoaaStations(current_ts)
        break;
    case 'obs':
        console.log('NOAA: Process Obs')
        getNoaaObs(current_ts)
        break;
    case 'all':
        console.log('NOAA: Process Stn')
        getNoaaStations(current_ts)
        console.log('NOAA: Process Obs')
        getNoaaObs(current_ts)
        break;

}