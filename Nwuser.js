var elasticsearch=require('elasticsearch');
var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);

var client = new elasticsearch.Client({  
    host: '10.71.71.176:9200',
    log: 'info'
});

var indexName = "movie_prediction4";
var doctype = "ratings" ;



app.get('/', function(req, res){
  res.sendFile(__dirname + '/newuser.html');
});


io.on('connection', function(socket){
  socket.on('userratings', function(msg){
	console.log(msg)

	var mid = [32,62,50,70,112,165,350,480];
	var res = msg.split(",");
	var ts = new Date().getTime();
	var count;
	client.count({index: indexName, type: doctype},function(err,resp,status) {  
	  	console.log("constituencies",resp);
		count=resp.count;
		var idnumber=count;

var aaaa={
  "fields": [
    "userId"
  ],
  "query": {
    "match_all": {}
  },
"size": 1,
"sort": [ { "userId" : { "order" : "desc"} } ]
}

console.log("My User ID : "+aaaa);

		//Inserting data at the last in ES
		for(var i = 0; i < 8; i++) 
		{
			idnumber+=1;
	 		console.log(i);
			client.index({  
				  index: indexName,
				  id: idnumber,
				  type: doctype,
				  body: {
				    "movieId": mid[i],
				    "userId": count,
				    "rating": res[i],
				    "timestamp": ts,
				  }
				},function(err,resp,status) {
				    console.log(resp);
			});
		
		}
		io.emit('userratings', "Your User ID is : "+count);
		io.emit('userratings', "Please enter this user Id in movie Recommendation");
	});
    
  });
});


http.listen(5000, function(){
  console.log('Server is listening on localhost:5000');
});


