const fs = require('fs')

const logFile = "/home/beenth12/www/ws/wxbox/node/weatherbox.log"

const formatMsg = (dataType,action,data) => {
    var outMsg = {
        dataType:dataType,
        action:action,
        data:data
    };
    return JSON.stringify(outMsg);
}

const formatDate = (date) => {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2) 
        month = '0' + month;
    if (day.length < 2) 
        day = '0' + day;

    return [year, month, day].join('-');
}

const logMessage = (stnId,msg) => {
    var d = (new Date()).toISOString().slice(0, 19).replace(/-/g, "/").replace("T", " ");

    var dMsg = d+" "+stnId+": "+msg
    console.log(dMsg)

    try {
        fs.writeFile(logFile, dMsg+"\n", { flag: 'a+' }, err1 => {
            if( err1 ) {
                console.log(d+": "+err1)
            }
        })
    } catch(err) {
        console.error(err)
    }
}

//distance
var in2mm = 25.4;
var mm2in = 0.0393701;
var ft2mm = 304.8;
var mm2ft = 0.00328084;     
var ft2m = 0.3048;
var m2ft = 3.28084;
var m2miles = 0.000621371;
var miles2m = 1609.33999997549;
var km2miles = 0.621371;
var miles2km = 1.60934;
var m2km = 0.001;
var km2m = 1000.0;
var miles2ft = 5280.0;
var ft2miles = 0.000189394;
var nm2ft = 6076.12;
var ft2nm = 0.000164579;
var m2nm = 0.000539957;
var nm2m = 1852;
var miles2nm = 0.868976;
var km2ft = 3280.84;
var km2nm = 0.539957;
var ft2km = 0.0003048;
var nm2km = 1.852;
var nm2miles = 1.15078;

function convertDistance(distUnitFROM,distUnitTO,distVal) {
    var outVal;
    switch(distUnitFROM) {
        case 'm':
            switch(distUnitTO) {
                case 'km':
                    outVal = distVal * m2km;
                    break;
                case 'miles':
                    outVal = distVal * m2miles;
                    break;
                case 'ft':
                    outVal = distVal * m2ft;
                    break;
                case 'nm':
                    outVal = distVal * m2nm;
                    break;
                default:
                    outVal = distVal;
            }
            break;
        case 'miles':
            switch(distUnitTO) {
                case 'km':
                    outVal = distVal * miles2km;
                    break;
                case 'm':
                    outVal = distVal * miles2m;
                    break;
                case 'ft':
                    outVal = distVal * miles2ft;
                    break;
                case 'nm':
                    outVal = distVal * miles2nm;
                    break;
                default:
                    outVal = distVal;
            }
            break;
        case 'km':
            switch(distUnitTO) {
                case 'miles':
                    outVal = distVal * km2miles;
                    break;
                case 'm':
                    outVal = distVal * km2m;
                    break;
                case 'ft':
                    outVal = distVal * km2ft;
                    break;
                case 'nm':
                    outVal = distVal * km2nm;
                    break;
                default:
                    outVal = distVal;
            }
            break;
        case 'ft':
            switch(distUnitTO) {
                case 'miles':
                    outVal = distVal * ft2miles;
                    break;
                case 'm':
                    outVal = distVal * ft2m;
                    break;
                case 'km':
                    outVal = distVal * ft2km;
                    break;
                case 'nm':
                    outVal = distVal * ft2nm;
                    break;
                default:
                    outVal = distVal;
            }
            break;
        case 'nm':
            switch(distUnitTO) {
                case 'miles':
                    outVal = distVal * nm2miles;
                    break;
                case 'm':
                    outVal = distVal * nm2m;
                    break;
                case 'km':
                    outVal = distVal * nm2km;
                    break;
                case 'ft':
                    outVal = distVal * nm2ft;
                    break;
                default:
                    outVal = distVal;
            }
            break;
        default:
            outVal = distVal;
    }
    return outVal;
}

function degrees_to_radians(degrees)
{
  var pi = Math.PI;
  return degrees * (pi/180);
}

function radians_to_degrees(radians)
{
    var pi = Math.PI;
    return radians * (180/pi);
}

const calcBoundingCoords = (wxLat,wxLon,wxDist,wxDistUnit,wxBrg) => {
    var R = 6378.1 
    var brng = degrees_to_radians(wxBrg)
    var d = convertDistance(wxDistUnit,"km",wxDist)
    
    lat1 = degrees_to_radians(parseFloat(wxLat))
    lon1 = degrees_to_radians(parseFloat(wxLon)) 
    
    lat2 = Math.asin( Math.sin(lat1)*Math.cos(d/R) + Math.cos(lat1)*Math.sin(d/R)*Math.cos(brng))
    
    lon2 = lon1 + Math.atan2(Math.sin(brng)*Math.sin(d/R)*Math.cos(lat1),Math.cos(d/R)-Math.sin(lat1)*Math.sin(lat2))
    
    lat2 = radians_to_degrees(lat2)
    lon2 = radians_to_degrees(lon2)

    var coordOut = {
        'wxLat':lat2,
        'wxLon':lon2
    }

    return coordOut
}

module.exports = {
    formatMsg,
    formatDate,
    logMessage,
    logFile,
    calcBoundingCoords,
}