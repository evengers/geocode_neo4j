/* see http://chrislarson.me/blog/install-neo4j-graph-database-ubuntu
The database can be cleared out by deleting the folder it is stored in.
cd /var/lib/neo4j/data
sudo rm -rf graph.db/
Restart neo4j
sudo /etc/init.d/neo4j-service restart
to get labels ...
curl http://localhost:7474/db/data/schema/index/


*/




var geocoder = require('simple-geocoder');
var co = require('co')
 , request = require('co-request');
var fs = require('fs-extra'); //var fs = require('fs')
var S = require('string');

var events = require('events');
var ev = new events.EventEmitter();

var batchCtr =0;
var inThisBatchCtr =0;
var rejectCtr =0;
var successCtr=0;

ev.on('getNextBatch', function (data, fn) {
    //console.log ("this data just in:" ,data); 
    console.log("time for another batch");
    inThisBatchCtr =0; 
    getSomeAddresses();  //keep on getting them until they are all done
});




//create and execute Cypher Query ... ask for next line when finished

//note 21000 were rejected on first pass
//3408 rejected on second pass

function setGeoResultsBad(resObj){

var setclause ='SET n.geoResults = "not-found"';
if (secondpass)setclause = 'SET n.geoResults = "not-found-3rdpass"';

var theStatements = [
'MATCH (n:LEI)',
'WHERE n.leiID = "'+resObj.ID+'"',
setclause,
'SET n.working = "lookUpGeoDone"',
'RETURN n',]; 

var query = theStatements.join('\n');

//console.log(query);
postQueryWithThisString(query);

};

var secondpass = true;


function setGeoResultsGood(resObj){
var theStatements = [
'MATCH (n:LEI)',
'WHERE n.leiID = "'+resObj.ID+'"',
'SET n.longitude = "'+resObj.longitude+'"',
'SET n.latitude = "'+resObj.latitude+'"',
'SET n.geoResults = "found"',
'SET n.working = "lookUpGeoDone"',
'RETURN n',]; 

var query = theStatements.join('\n');

//console.log(query);
postQueryWithThisString(query);

};



function getSomeAddresses(){
batchCtr = batchCtr + 50; 
console.log ("batch: ", batchCtr/50, "  ... total requests: ", batchCtr);

var whereclause ='WHERE n.working = "idle"';
if (secondpass)whereclause = 'WHERE n.geoResults = "not-found-2ndpass"';
var theStatements = [
'MATCH (n:LEI)',
whereclause,
'SET n.working = "lookingUpGeo"',
"RETURN n.leiID, n.address LIMIT 50",]; 

var query = theStatements.join('\n');

//console.log(query);
requestQueryWithThisString(query);

};



function requestQueryWithThisString(astr) {

//console.log ("using querystr: ", astr);

var bodyAsObj = {"query": astr};
var bodyAsStr = JSON.stringify(bodyAsObj);

//theurl = 'http://localhost:7474/db/data/index/node/'+ thelabel + '?uniqueness=get_or_create';
 // uri: 'http://0.0.0.0:7474/db/data/cypher',

function *askMe(){

 var theurl = 'http://0.0.0.0:7474/db/data/cypher';
  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    body: bodyAsStr
  });
  
  var response = result;
  var responsebody = result.body;

// console.log("got this response ... ", responsebody);
var arespObj = JSON.parse(responsebody);
var somefeedback = arespObj.data;
  //console.log("got this feedback ... ", somefeedback);
return somefeedback;  
};

co(function *(){
 var results = yield askMe();
   //console.log("got this back ... ", results);
 if (results) {   
      var a = results;
      a.forEach(function(entry) {
      // console.log ("theText: ",entry[1]," theID: ",entry[0]);
       getGeoWith(entry[1], entry[0]);
      });
      
      return results;
   }

//if(!indexesAreNotSet) ev.emit('getNextLine', "optional send something on event", function(){});

})();

};


function postQueryWithThisString(astr) {

//console.log ("using querystr: ", astr);

var bodyAsObj = {"query": astr};
var bodyAsStr = JSON.stringify(bodyAsObj);

//theurl = 'http://localhost:7474/db/data/index/node/'+ thelabel + '?uniqueness=get_or_create';
 // uri: 'http://0.0.0.0:7474/db/data/cypher',

function *askMe(){

 var theurl = 'http://0.0.0.0:7474/db/data/cypher';
  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    body: bodyAsStr
  });
  
  var response = result;
  var responsebody = result.body;

// console.log("got this response ... ", responsebody);
var arespObj = JSON.parse(responsebody);
var somefeedback = arespObj.data;
  //console.log("got this feedback ... ", somefeedback);
return somefeedback;  
};

co(function *(){
 var results = yield askMe();
   //console.log("got this back ... ", results);
 if (results) {   
      var a = results;      
      return a;
   }

//if(!indexesAreNotSet) ev.emit('getNextLine', "optional send something on event", function(){});

})();



};





// '18 Waldstrasse, Eppstein'  gets 8.369492811000441 50.17801492900048

getSomeAddresses();

function getGeoWith(thisString, thisID){

if ((typeof thisString === 'undefined') || (typeof thisID === 'undefined')) {
  //console.log ("UNDEFINED csv object");
  logRejects ("line: " + ctr + " is undefined ");
  console.log ("using this address", thisString);
   console.log ("using this id", thisID);
}else{
 thisString = S(thisString).replaceAll('\n',', ').s;
   if (secondpass){
      //chomp off the fist part of string and try again
      var aStrArray = []; 
      console.log("before: ", thisString);
       aStrArray = thisString.split(', ');
       aStrArray.shift();//just this for 2nd pass
       aStrArray.shift();//this as well for 3rd
       thisString = aStrArray.join(', ');
      console.log("after: ", thisString)
   };


geocoder.geocode(thisString, function(success, locations){
  var resObj ={};
  resObj.ID = thisID;
  if (!success){
        //console.log ("error: ", locations);
        logGeosReject (thisID )
        setGeoResultsBad(resObj);
     };
  if(success) {     
      resObj.longitude = locations.x;
      resObj.latitude = locations.y;
      var outTxt= resObj.ID + "\t" + resObj.longitude + "\t" + resObj.latitude;
      //console.log(outTxt);
      logGeos ( outTxt);
      setGeoResultsGood(resObj);
      
   };
});

}//end if .. else
};



function logGeos ( text ) 
{     
 successCtr++;
inThisBatchCtr++;
if (inThisBatchCtr>48) ev.emit('getNextBatch', "optional", function(){});

  fs.open('geos.txt', 'a', function( e, id ) {
   fs.write( id, text + "\n", null, 'utf8', function(){
    fs.close(id, function(){
     //console.log('geos file is updated');
    });
   });
  });
 };

function logGeosReject ( text ) 
{     
  rejectCtr++;
  inThisBatchCtr++;
if (inThisBatchCtr>48) ev.emit('getNextBatch', "optional", function(){});


  fs.open('geosReject.txt', 'a', function( e, id ) {
   fs.write( id, text + "\n", null, 'utf8', function(){
    fs.close(id, function(){
     console.log('so far REJECTed: ', rejectCtr, "found: ", successCtr, " in this batch:", inThisBatchCtr);
    });
   });
  });
 };
