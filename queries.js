const fs = require("fs");
const path = require('path')
const Moment = require('moment-timezone')
const settings = require('./settings')
const exec = require('child_process').exec;

//Moment.tz.setDefault('Asia/Seoul')

const mode = settings.mode

const DEBUG = (msg) =>{
	if(mode == 1)
		console.log(msg)
}


//for prop read
const PropertiesReader = require('properties-reader');
var parser = require('properties-file')
let prop
getProperty = (pty) => {return prop.get(pty);}

const Pool = require('pg').Pool

const pool = new Pool( settings.dbSetting )

let propDefault = settings.propDefault
//timeout
let timeout



const funcQuery = (sql,data, func) =>{
	pool.query(sql,data, func)
}

	
const simpleQuery = (sql, res, flag, data) => {

	pool.query(sql, data, (err, results) => {

		console.log(results)
		checkErr(err, res)
		if(flag == 0)	//resp all
		{
			res.statusCode = 200
			res.send(results.rows)
			res.end()
		}
		else if(flag == 1){

			console.log("status 204")
			res.status(204).end()
		}

	})
}

const checkErr = (err, res) =>{
	if(err)
	{
		console.log(err)
		if(typeof res !== 'undefined')
			res.status(404).end()
		throw err
	}
}

const convertDate = (d) =>{
	return Moment(d).format('YYYY-MM-DD HH:mm:ss')
}

const readPropAll = (rtv) =>{
	let tempObj={}
	//	let tempPropAry= [...propAry];
	for(let item of settings.propAry){
		let temp = getProperty(item)
			//	console.log(temp)
			if(temp != null){
				if(item === 'recordcount')
					propDefault['insertcount'] = temp
				tempObj[item] = temp
			}
			else{
				tempObj[item] = propDefault[item]
			}
	}
	rtv.propValues = tempObj

}

const getWorkload = (req, res) => {
		let sql = 'SELECT * FROM sk.workload WHERE w_id=$1'
		DEBUG(sql)

		funcQuery( sql,[req.query.w_id], (err, results)=>{
			console.log(results)
			checkErr(err, res)
			let rtv = results.rows[0]
			prop = PropertiesReader(rtv.w_config_file_path)
			//all props read from workload file
			readPropAll(rtv)
			res.statusCode = 200
			res.send(rtv)
			res.end()
		})

}

const getWorkloads = (req, res) => {
		let sql = 'SELECT '+
			'w_id, w_name, w_read, w_insert, w_update, w_readmodifywrite, w_scan, w_c_timestamp, w_freq_count, w_error_count, w_d_flag '+
			'FROM sk.workload '+
//			'WHERE w_d_flag=0 '+ 
			'ORDER BY w_id ASC'

		DEBUG(sql)
		simpleQuery(sql, res, 0)
}

const insertAndSelect = (res, data) => {
	//#fix me F-id
	let insertSql = 'INSERT INTO sk.workload '+
	'(w_name, w_config_file_path, w_read, w_insert, w_update,  w_readmodifywrite, w_scan, w_c_timestamp, w_freq_count, w_error_count, w_d_flag) '+
	'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 0, 0)'

	let selectSql =  'SELECT '+
	'w_id, w_name, w_read, w_insert, w_update, w_readmodifywrite, w_scan, w_c_timestamp, w_freq_count, w_error_count, w_d_flag '+
	'FROM sk.workload '+
	'WHERE w_c_timestamp= \''+data[7]+'\''

	DEBUG("insertAndSelect")
	funcQuery(insertSql, data, (err, results) =>{		
		checkErr(err, res)
		simpleQuery(selectSql, res, 0)

	})

}

const insertAndSelectBench = (res, data) =>{
	let insertSql = 'INSERT INTO sk.run(r_s_timestamp, r_e_timestamp, r_c_timestamp, r_status_flag, r_window_size, r_threshold, r_name) VALUES(null, null, $1, 0, $2, $3, $4)'
	let selectSql = 'SELECT r_id FROM sk.run WHERE r_c_timestamp=\''+data[0]+'\''

	DEBUG(insertSql)
	DEBUG(selectSql)
	return new Promise(function (resolve, reject) {
		funcQuery(insertSql, data, (err, ir) =>{
			console.log("data = " + data)
			DEBUG(ir)
			checkErr(err, res)

			funcQuery(selectSql, [], (err, results) =>{
				console.log("select= " )
				checkErr(err, res)
				DEBUG(results.rows)
				if(results.rows.length > 0)
					resolve(results.rows[0].r_id)
				else
					reject(new Error("Select Request Failed"))
			})
		})
	})
}

const writePropfile = (res, data) =>{
	fs.writeFile(data.filePath, parser.stringify(data.wlFileProps), (err) => {
		checkErr(err, res)
	})

}

//워크로드 추가 & update
const modifyWorkload = (req, res) => {
		var date = new Date()
		let convD = convertDate(date)



		DEBUG(req.body)
		console.log(req.body)
		//insert
		if(req.body.data.modifyFlag == 0){
			
			let wName = req.body.data.w_name
//			let fsize = req.body.data.
			let wlFileProps = req.body.data.propValues
			let rp = wlFileProps.readproportion
			let up = wlFileProps.updateproportion
			let sp = wlFileProps.scanproportion
			let ip = wlFileProps.insertproportion
			let rmwp = wlFileProps.readmodifywriteproportion
			let filePath = settings.propPath+wName+date.getTime()
			let data = [ wName, filePath,rp, up, ip, sp, rmwp, convD]
			
/*
			if(size > 0){
				//
				//file Table // f-id	
			}
			
*/
			
			writePropfile(res, 
			{
				"filePath" : filePath,
				"wlFileProps" : wlFileProps
			})

			insertAndSelect(res, data)

		//modify
		}else if(req.body.data.modifyFlag == 1){
			let wName = req.body.data.w_name
			let wlFileProps = req.body.data.propValues
			let rp = wlFileProps.readproportion
			let up = wlFileProps.updateproportion
			let sp = wlFileProps.scanproportion
			let ip = wlFileProps.insertproportion
			let rmwp = wlFileProps.readmodifywriteproportion
			let filePath = settings.propPath+wName+date.getTime()
			let data = [ wName, filePath,rp, up, ip, sp, rmwp, convD]
			let wId = req.body.data.w_id
			let checkSql = 'SELECT w_freq_count, w_config_file_path FROM sk.workload WHERE w_id = $1'
			DEBUG(checkSql)
			funcQuery( checkSql, [wId], (err, results) =>
			{
				
				checkErr(err, res)
				//update
				if(results.rows[0].w_freq_count == 0)
				{

					writePropfile(res, 
					{
						"filePath" : results.rows[0].w_config_file_path,
						"wlFileProps" : wlFileProps
					})
					//#FIXE ME = File Insert
					let updateSql =  'UPDATE sk.workload '+
	'SET w_name=$1, w_read=$2, w_insert=$3, w_update=$4, w_readmodifywrite=$5, w_scan=$6 '+
	'WHERE w_id = $7'
					DEBUG(updateSql)

					simpleQuery(updateSql, res, 1 , [wName, rp, ip, up, rmwp, sp, wId])
						
				//create 	
				} else{
					let updateSql = 'UPDATE sk.workload '+
	'SET w_d_timestamp = now(), w_d_flag = 1 '+
	'WHERE w_id = $1'
					
					DEBUG(updateSql)

					funcQuery(updateSql, [wId], (err, results) =>{
						checkErr(err, res)
						writePropfile(res, 
						{
							"filePath" : filePath,
							"wlFileProps" : wlFileProps
						})
						insertAndSelect(res, data)
					})
				}
				
			})
		//copy
		}else{
			let wId = req.body.data.w_id
			let checkSql = 'SELECT * FROM sk.workload WHERE w_id = $1'		
			console.log(req.body)		
			DEBUG(checkSql)
			funcQuery( checkSql, [wId], (err, results) => {
				checkErr(err, res)
				let row = results.rows[0]
				let name = row.w_name+'_copy'
				let filePath = settings.propPath+name+date.getTime()
				fs.copyFile(row.w_config_file_path, filePath, (err) =>
				{
					checkErr(err, res)
				})
				
				let data = [ name, filePath, row.w_read, row.w_insert, row.w_update,  row.w_readmodifywrite, row.w_scan, convD]
				insertAndSelect(res, data)	
			})
		}
		

}

const getResults = (req, res) => {
	let sql = 'SELECT * FROM sk.run'
	DEBUG(sql)
	let rtv = null
	funcQuery(sql, [], (err, run_results) =>{
		checkErr(err, res)
		rtv = run_results.rows
		sql = 'SELECT r_id, n_id, w_id FROM sk.run_relation ORDER BY r_id, n_id'	
		funcQuery(sql, [], (err, results) =>{

			checkErr(err, res)
			if(results.rows.length > 0 ){
				let _rid = -1// results.rows[0].r_id
				let _nid = -1//results.rows[0].n_id
				let w_ids=[]
				
				//prev rid, node id, flag(row, node), new rid, node id, workload
				itemFind = (flag, _nr, _nn, _nw)  => {
					rtv.forEach( r => {
						if(r.r_id === _rid)	{
							if(typeof r.relation === 'undefined')
								r.relation = []
							r.relation.push({'n_id' : _nid, 'w_ids' : w_ids})
							return	
						}
					})
					if(flag === 0)
						_rid = _nr
					_nid = _nn
					w_ids = [_nw]

				}
				results.rows.forEach(row=>{

					if(row.r_id != _rid) {
						itemFind(0, row.r_id, row.n_id, row.w_id)
					}else if(row.n_id != _nid){
						itemFind(1, row.r_id, row.n_id, row.w_id)
					}else{
						w_ids.push(row.w_id)					
					}		
				})
				if(w_ids.length > 0)
					itemFind(0, -1, -1, -1)
			}		

			res.statusCode = 200
			res.send(rtv)
			res.end()

		})

	})

}

const readFileResult = rows =>{
	return new Promise(function (resolve, reject) {
		for(let i = 0; i < rows.length; i++){
			let data = fs.readFileSync(rows[i].ycsb_result_path).toString()
//				checkErr(err)
//			console.log(data)
			rows[i].ycsb = JSON.parse(data)
//				console.log(row.ycsb)
		
		}
		console.log(rows)
		resolve(rows)
	})
}

const getResultY = (req, res) => {

	let sql = 'SELECT n_id, w_id, ycsb_result_path FROM sk.run_relation rr, sk.ycsb_result yr WHERE rr.r_id = $1 AND rr.rr_id = yr.rr_id'
	let rId = req.query.r_id;
	console.log("rid="+rId)
	DEBUG(sql)
	funcQuery(sql, [rId], (err, results) => {
		checkErr(err, res)
		readFileResult(results.rows).then( rows =>{
			res.statusCode = 200
			res.send(rows)
			res.end()
		})
	})	
}


const getDBs = (req, res) => {
		
	let sql = 'SELECT * FROM sk.nosql'
	DEBUG(sql)
	simpleQuery(sql, res, 0)
}

const deleteWorkload = (req, res) => {

	let sql = 'UPDATE sk.workload SET w_d_flag = 1, w_d_timestamp = now() WHERE w_id in ('+req.query.w_ids+')'

	console.log( req.query)
	DEBUG(sql)
	simpleQuery(sql, res, 1)
}

const n_Check = (times, result_file, check_file, rId, runYCSB) => {
//	let timeout//setTimeout을 clearTimeout하기 위한 역할의 변수
	if(times<1){
		return
	}

	function loop(){//해당 loop는 상태점검 결과파일이 생성될 때까지 실행을 잠시 멈추는 코드
		setTimeout(()=>{
			if(fs.existsSync(result_file)){//NoSQL상태점검 결과파일이 생성될 디렉터리 상 파일
			return;
			}else{
				console.log('NoSQL 상태점검 완료 대기 중.')
				loop()
			}
		}, 500)
	}

	loop()//상태점검파일이 디렉터리에 생길 때까지 실행을 멈춤.

	timeout = setTimeout(function(){
		
		if(times==1){//여기서 타임을 검사하는 이유는 node.js가 asynchronous하게 동작하기 때문에
			var date = new Date()
			let convD = convertDate(date)
			let sql = 'UPDATE sk.run SET r_status_flag=2, r_e_timestamp=now() WHERE r_id = $1'
			simpleQuery(sql, null, 2, [rId])	

			//res.status(404).end()
			console.log('Failed to re-run NoSQL.')
			return
		}
		console.log("result = " + result_file)
		fs.readFile(result_file, function(err, data){//nosql.json은 상태점검결과 파일에 상응함.
			checkErr(err)

			data = JSON.parse(data)
			if(data.result === 'active'){
				console.log('NoSQL in stable state. Benchmark will soon begin..')
				clearTimeout(timeout)

				//YCSB 실행 코드가 들어갈 부위.
				exec('java -jar '+runYCSB+' '+rId, function(err, stdout, stderr){//runnable jar 파일로 만들어줘야 실행이 가능함. java(X), 그냥 jar도 (X)

					console.log("java run")
					checkErr(err)
				})//자바 파일 실행하는 코드*/
				return	
			}else{
				console.log('Re-running requested nosql. Please wait for a moment.')//nosql에 이상이 있어서 재구동 중임.
				var script = exec(check_file, (err, stdout, stderr)=>{
					//아래 코드는 파일을 종료시키고 나서 실행되는 코드. (file close)
					checkErr(err)
				})
				n_Check(times-1, result_file, check_file, rId, runYCSB)
			}
		})

	}, 3000)
}



const executeBenchmark = (req, res) => {
	
	var date = new Date()
	let convD = convertDate(date)
	let r_id

	let r_window_size = req.body.data.r_window_size
	let r_threshold = req.body.data.r_threshold

	if(typeof r_window_size === 'undefined')
		r_window_size = 10
	if(typeof r_threshold === 'undefined')
		r_threshold = 1000

	let r_name = req.body.data.r_name
	let settings = req.body.data.benchmarks
	console.log(req.body)
	console.log(settings.w_ids)

	let checkNosql= '/home/skhm/song_test/checkNosql.sh 0'//NoSQL상태점검 스크립트 파일명(경로 명시 필요)
	let runYCSB='/home/skhm/test_sb/App.jar'
	let nodeStatus='/home/skhm/song_test/result.json'

	let times=5//setTimeout 내 함수 반복 횟수
	
	
	insertAndSelectBench(res,[convD, r_window_size, r_threshold, ''+r_name]).then(rId => {
		console.log("rid = " + rId)
		let sql = 'INSERT INTO sk.run_relation (r_id, n_id, w_id) VALUES($1, $2, $3)'

		settings.forEach(db =>{
			db.w_ids.forEach(wid =>{
				funcQuery(sql, [rId, db.n_id, wid], (err, results)=>{
					checkErr(err, res)
				})
			})
		}) 
		res.status(204).end()

		
		exec(checkNosql, (err, stdout, stderr)=>{
			//아래 코드는 파일을 종료시키고 나서 실행되는 코드. (file close)
			checkErr(err)
		})

		n_Check(times, ''+nodeStatus, checkNosql, rId, runYCSB)
	}).catch( err =>{
		res.status(404).end()
		throw err
	})

	    
}



module.exports = {
	getWorkloads,
	getWorkload,
	deleteWorkload,
	modifyWorkload,
	getDBs,
	getResultY,
	getResults,
	executeBenchmark,
}
