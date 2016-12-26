var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);

app.get('/', function(req, res){
  res.sendFile(__dirname + '/displayoutput.html');
});

io.on('connection', function(socket){
  socket.on('display', function(msg){
	console.log(msg)
	
	const spawn = require('child_process').spawn;
	var ls = spawn('java', ['-cp', '/home/ganesh/Desktop/HelloES/out/artifacts/helloes_jar/helloes.jar', 'movieRecommendation', msg]);
	ls.stdout.on('data', (data) => {
	console.log(`${data}`);
	io.emit('display', `${data}`);
	});

    
  });
});

http.listen(2000, function(){
  console.log('Server is listening on localhost:2000');
});


