const express = require('express');
const app = express();
const bodyParser = require('body-parser')
const http = require("http").Server(app);
const io = require('socket.io')(http);
const user = 0;
const db = require('./queries')
const settings = require('./settings')

let  router = express.Router();


//for cors
const cors = require('cors');

app.use(express.json())
//app.use(express.urlencoded())
app.use('/', router)


	//for cors
app.use(cors());


app.get('/getWorkloads', db.getWorkloads)
app.get('/getWorkload', db.getWorkload)
app.post('/modifyWorkload', db.modifyWorkload)
app.delete('/deleteWorkload', db.deleteWorkload)
app.get('/getDBs', db.getDBs)


var server = http.listen(settings.port, settings.host,  function() {
		io.on('connection', ()=>{
			console.log('user '+user+' has connected')
			++user;
		})
console.log("Server running at http://%s:%s", settings.host, settings.port)
//console.log(Moment(new Date()).format('YYYY-MM-DD HH:mm:ss'))


})

module.exports = router;
