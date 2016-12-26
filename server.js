var elasticsearch=require('elasticsearch');
var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);

var count=0;
var client = new elasticsearch.Client( {  
  hosts:[
    '10.71.71.176:9200'
  ]
});

module.exports = client;  


app.get('/', function(req, res){
  res.sendFile(__dirname + '/ui.html');
});

var indexName = new Array("per_movie_count","per_movie_avg","highest_rating_review","per_genres_highest_movies");
var doctype = new Array("PerMovieCount","PerMovieAvg","HighestRatingReview","PerGenresHighestMovies");
var msg = ""
var extuid = new Array();
io.on('connection', function(socket){
	
	var mid = new Array();
	socket.on('showtable', function(msg){		
		console.log(msg)
		const spawn = require('child_process').spawn;		
			var ls = spawn('/usr/local/src/spark-1.6.2-bin-hadoop2.6/bin/spark-submit' , ['--class','movieRecommendation','--master','spark://thrush:7077','/home/ganesh/Desktop/Project_Jar/helloes_jar/helloes.jar']);

			var output="";
			var op="";
		ls.stdout.on('data', (data) => {
			
			output+=`${data}`;
			console.log("ID "+`${data}`);					
			io.emit('showtable', output);
			console.log(output);	

			op+=`${data}`;
			var opid=op.split("@");
			//if(output.length>1)
			//console.log(output);
			//var output1=output.split("@");
			//if(output1.length>1)
			//console.log(output1[1]);		
			var i=1;
			var j=0;
			while(i<opid.length){
				mid[j]=Number(opid[i]);
							
				i=i+2;
				j=j+1;
			}
			console.log(mid);								
		});								
			
	});
	
	socket.on('showmovie', function(msg){		
		console.log(msg)
		extuid[0]=Number(msg);
		const spawn = require('child_process').spawn;		
			var ls = spawn('/usr/local/src/spark-1.6.2-bin-hadoop2.6/bin/spark-submit' , ['--class','movieRecommendation','--master','spark://thrush:7077','/home/ganesh/Desktop/Project_Jar/Ex_usr_rating_jar/helloes.jar',msg]);

			var output="";
			var op="";
		ls.stdout.on('data', (data) => {
			
			output+=`${data}`;
			console.log("ID "+`${data}`);					
			io.emit('showmovie', output);
			console.log(output);	

			op+=`${data}`;
			var opid=op.split("@");
			//if(output.length>1)
			//console.log(output);
			//var output1=output.split("@");
			//if(output1.length>1)
			//console.log(output1[1]);		
			var i=1;
			var j=0;
			while(i<opid.length){
				mid[j]=Number(opid[i]);
							
				i=i+2;
				j=j+1;
			}
			console.log(mid);								
		});								
			
	});
	

	socket.on('userratings', function(msg)
	{
		console.log(msg)	
		var res = msg.split(",");
		var ts = new Date().getTime();
	
		client.search({  
		  index: "movie_prediction_final",
		  type: "ratings",
		  body: {
		    query: {
		      match_all: {}
		    },
		     "sort": [
		   {
		     "userId": {
		       "order": "desc"
		     }
		   }
		 ],
		 "size": 1
		  }
		},function (error, response,status) {
		    if (error){
		      console.log("search error: "+error)
		    }
		    else 
		    {
		      
		      response.hits.hits.forEach(function(hit){
			       //var data = JSON.parse(hit);
		
				//console.log(data.key)
				console.log(hit._source.userId)
				count=Number(hit._source.userId);
				count+=1;
				console.log("Count "+count);
			    
	

					console.log("Count "+count);
				//Inserting data at the last in ES
				for(var i = 0; i < 8; i++) 
				{
					//idnumber+=1;
			 		console.log(i);
					client.index({  
						  index: "movie_prediction_final",
						  type: "ratings",
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
				io.emit('userratings', "Please enter this user Id in movie Recommendation system");
		  	  })
    			}
		}); 
		var cntind;
		cntind=0;			
		for(cntind=0;cntind<4;cntind++)
		{			
			myfunction(indexName[cntind],doctype[cntind],cntind);
		}
		
		function myfunction(indexName,doctype,flag)
		{			
			console.log(indexName);
				client.indices.exists({index: indexName},function(err,resp,status) { 
				  if(resp)
				  {
					client.indices.delete({index: indexName},function(err,resp,status) {  
				  	console.log("delete",resp);
					});
					client.indices.create({  
				  index: indexName
				},function(err,resp,status) {
				  if(err) {
				//   console.log("ER")
				    console.log(err);
				  }
				  else {
					client.indices.putMapping({
					index: indexName,
					type: doctype,
					body: {
					    properties: {
						title: { type: "string" },
						count: { type: "integer" },
						movieId: { type: "integer" },
						average: { type: "integer" },
						genres: { type: "string" }
					    }

					}
				    });
				    console.log("create",resp);
					if(flag==3)
					{
						var options = {
			    				stdio: 'inherit' //feed all child process logging into parent process
						};
						const spawn = require('child_process').spawn;
				var ls = spawn('java', ['-jar', '/home/ganesh/IdeaProjects/MovieTransformation/out/artifacts/movietransformation_jar/movietransformation.jar'],options);
						
						
					}	
				  }
				});

	
				  }
				});
			
		}		    
	});


	socket.on('userratings1', function(msg)
	{			
		console.log("-------------------------"+extuid[0]);
		var res = msg.split(",");
		var ts = new Date().getTime();	

		//Inserting data at the last in ES
		for(var i = 0; i < 8; i++) 
		{
			//idnumber+=1;
	 		console.log(i);
			client.index({  
				  index: "movie_prediction_final",
				  type: "ratings",
				  body: {
				    "movieId": mid[i],
				    "userId": extuid[0],
				    "rating": res[i],
				    "timestamp": ts,
				  }
				},function(err,resp,status) {
				    console.log(resp);
			});

		}
		io.emit('userratings1', "Your data is stored successfully");
		//io.emit('userratings', "Please enter this user Id in movie Recommendation system");		  	 
	});





 socket.on('chat message', function(number){
        msg = number
	console.log(msg)
    io.emit('chat message', number);
if(msg != ""){
  if(msg == "1")
    {
	indexName = "per_movie_count";
        doctype = "PerMovieCount";
    }
    else if(msg == "2")
    {
	indexName = "per_movie_avg";
        doctype = "PerMovieAvg";
    }
    else if(msg == "3")
    {
	indexName = "per_movie_avg_highest";
        doctype = "PerMovieAvgHighest";
    }
    else if(msg == "4")
    {
	indexName = "highest_rating_review";
        doctype = "HighestRatingReview";
    }
    else if(msg == "5")
    {
	indexName = "per_genres_highest_movies";
        doctype = "PerGenresHighestMovies";
    }	
   console.log(indexName)
   console.log(doctype)	
  
  var count = 0
var intervalObject = setInterval(function(){
var message = ""
client.search({  
  index: indexName,
  type: doctype,
  body: {
    query: {
      match_all: {}
    },
  }
},function (error, response,status) {
    if (error){
      console.log("search error: "+error)
    }
    else {
      console.log("--- Response ---");
      //console.log(response);
      console.log("--- Hits ---");
      
      response.hits.hits.forEach(function(hit){
       //var data = JSON.parse(hit);
       if(msg == "1")
		message = hit._source.title+","+hit._source.count+","+hit._source.genres
        else if(msg == "2" || msg == "3" || msg == "4")
		message = hit._source.title+","+hit._source.average+","+hit._source.genres
        else if(msg == "5")
		message = hit._source.title+","+hit._source.genres
       	if(!((message == "undefined undefined") ||(message =="\"Mask undefined")) && (count!=10))
        {	
		socket.emit('data', message);
		count++;
	        console.log(message)
        }      
	console.log(count)
	 if (count == 5) { 
            console.log('exiting'); 
            clearInterval(intervalObject); 
        } 
	//console.log(data.key)
	//console.log(hit)
      })
    }
});

 }, 1000);}
});



  socket.on('User ID', function(msg){
	console.log(msg)
	
	const spawn = require('child_process').spawn;
	//var ls = spawn('java', ['-cp', '/home/ganesh/IdeaProjects/HelloES/out/artifacts/helloes_jar/helloes.jar', 'movieRecommendation', msg]);
	var ls = spawn('/usr/local/src/spark-1.6.2-bin-hadoop2.6/bin/spark-submit' , ['--class','movieRecommendation','--master','spark://thrush:7077','/home/ganesh/IdeaProjects/HelloES/out/artifacts/helloes_jar/helloes.jar', msg]);

//var ls = spawn('/usr/local/src/spark-1.6.2-bin-hadoop2.6/bin/spark-submit' , ['--class','movieRecommendation','--master','spark://thrush:7077','/home/ganesh/Desktop/Project_Jar/Display_all/helloes.jar', msg]);



	ls.stdout.on('data', (data) => {
		var output=`${data}`.split("@");
		console.log(`${data}`);
		var i=0;
		while(i<output.length){
			io.emit('User ID', output[i]);
			i=i+1;
		}
	});
    
  });
});

http.listen(3000, function(){
  console.log('Server is listening on localhost:3000');
});



//   /home/ganesh/Desktop/Project_Jar/helloes_jar
