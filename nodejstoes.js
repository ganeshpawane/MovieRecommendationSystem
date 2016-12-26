var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({  
    host: '172.20.25.191:9200',
    log: 'info'
});

var indexName = "new_user_index";
var doctype = "new_map" ;

client.index({  
  index: indexName,
  id: '1',
  type: doctype,
  body: {
    "movieId": 1111,
    "userId": 1,
    "rating": 4.5,
    "timestamp": 7449956894,
  }
},function(err,resp,status) {
    console.log(resp);
});

/*

//Delete Index

client.indices.delete({index: indexName},function(err,resp,status) {  
  console.log("delete",resp);
});
*/

/*

//Create Index

client.indices.create({  
	index: indexName
},function(err,resp,status) {
  if(err) {
    console.log(err);
  }
  else {
	client.indices.putMapping({
        index: indexName,
        type: doctype,
        body: {
            properties: {
                movieId: { type: "integer" },
                userId: { type: "integer" },
                rating: { type: "double" },
                timestamp: { type: "long" }
            }
        }
    });
    console.log("create",resp);
  }
});

*/	

