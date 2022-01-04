var Pool = require('pg-pool')
const { Client } = require('pg')
const fetch = require('sync-fetch')
var async = require('async')


const https = require('https'); 
const fs = require('fs');
const { formatWithOptions } = require('util');

const noaaNwsDb = new Pool({
    user: 'root',
    host: '127.0.0.1',
    database: 'stn_other',
    password: 'SailView2627!',
    port: 5432,
    max: 500,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 120000,
    maxUses: 10,
})

async function getNwsStations(startTime) {
  try {
    var tDate = new Date(1970, 0, 1)
    tDate.setSeconds(startTime)
    var timeString = tDate.toTimeString()
    console.log("NWS: Stn Startup => "+timeString)

    var states = ["AL","AK","AS","AR","AZ","CA","CO","CT","DE","DC","FL","GA","GU","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","PR","RI","SC","SD","TN","TX","UT","VT","VI","VA","WA","WV","WI","WY"]

    var apiQry = "api.weather.gov/stations"

    var index = 0

    async.each(states, async function(myState) {
        const options = {
            host: "api.weather.gov",
            path: "/stations?state="+myState,
            headers: {
                'Accept': 'application/geo+json;version=1',
                'User-Agent': '"http://beentheresailedthat.com (admin@beentheresailedthat.com)'
            }
        }
        var myFile = process.cwd()+"/nws/"+myState+".json"

        fs.stat(myFile, (error, stats) => {
            if (!error) {
              fs.unlink(myFile, (err => {
                if (err) console.log("NWS: ERROR2 => "+err)
              }))
            }
        });
        // const file = fs.createWriteStream(myFile)
        var stnStr = ""
        const request = https.get(options, function(response) {
            response.on('data', function (chunk) {
                stnStr += chunk
            });
        
            response.on('end', function () {
                // fs.writeFile(myFile,stnStr,(err) => {
                //     if (err)
                //       console.log("NWS: ERROR3 => "+err)
                //     else {
                //         var stnsText = fs.readFileSync(myFile)
                        if( stnStr != "") {
                            var stnsJson = JSON.parse(stnStr)
                
                            var stationsInfo = stnsJson.features

                            async.each(stationsInfo, async function(myStationInfo) {
                
                                var stnId = myStationInfo.properties.stationIdentifier
                                var stnName = myStationInfo.properties.name.replace(new RegExp("'", 'g'), "''")
                                stnName = stnName.replace(new RegExp("\"", 'g'), "")
                                var stnLoc = myStationInfo.geometry.coordinates
                
                                var stnType = "NWS"
                
                                var sql_base = "insert into wx_stn (stn_id,stn_lat,stn_lon,stn_name,stn_type) values "
                                var sql_data = "("
                                    sql_data = sql_data+"'"+stnId+"',"
                                    sql_data = sql_data+"'"+stnLoc[0]+"',"
                                    sql_data = sql_data+"'"+stnLoc[1]+"',"
                                    sql_data = sql_data+"'"+stnName+"',"
                                    sql_data = sql_data+"'"+stnType+"'"
                                    sql_data = sql_data+")"
                                
                                var sql_end = " ON CONFLICT (stn_id) DO NOTHING;"
                
                                var noaaNwsT = await noaaNwsDb.connect()
                                try {
                                    var sql = sql_base+sql_data+sql_end
                                    await noaaNwsT.query(sql)
                                } catch(e) {
                                    console.log('NOAA: Stn Error1 ('+index+') => ', e)
                                } finally {
                                    await noaaNwsT.release()
                                }
                            }) 
                        }
                //     }
                // })
            })
        })
    })
    console.log("NWS: Exiting Stn...")
  }
  catch(e) {
    console.log('NWS: Stn Error => ', e)
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
            var stationID = stnLine.substring(0,5).trim()
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
        getNwsStations(current_ts)
        break;
    case 'obs':
        console.log('NOAA: Process Obs')
        getNoaaObs(current_ts)
        break;
    case 'all':
        console.log('NOAA: Process Stn')
        getNwsStations(current_ts)
        console.log('NOAA: Process Obs')
        getNoaaObs(current_ts)
        break;

}