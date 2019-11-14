const mode = 0// 1 = debug, 0 = run
const host = "203.255.92.192"
const port =  mode == 1 ?  8082 : 80
const propPath = '/home/skhm/YCSB/workloads/'
const dbSetting = {
	user: 'skhm',
	host: '203.255.92.192',
	database: 'skdb',
	password: 'netdb3230',
	port: 5432,
}

const propAry = ['recordcount', 'operationcount', 'readallfields', 'readproportion', 'updateproportion', 'scanproportion', 'insertproportion', 'readmodifywriteproportion', 'fieldcount', 'fieldlength', 'minfieldlength', 'writeallfields', 'requestdistribution', 'minscanlength','maxscanlength', 'scanlengthdistribution','insertstart','insertcount','zeropadding','insertorder','fieldnameprefix', 'threadcount','target', 'hdrhistogram.percentiles']

let propDefault = {'readallfields' : true, 
 'readproportion' : 95, 
 'updateproportion' : 5, 
 'scanproportion' : 0,
 'insertproportion' : 0,     
 'readmodifywriteproportion' : 0 ,
 'fieldcount' : 10 , 
 'fieldlength' : 100, 
 'minfieldlength' : 1, 
 'writeallfields' : false, 
 'requestdistribution' : 'uniform', 
 'minscanlength' : 1,
 'maxscanlength' : 1000, 
 'scanlengthdistribution' : 'uniform',
 'insertstart' : 0, 
 'zeropadding' : 1,
 'insertorder' : 'hashed',
 'fieldnameprefix' : 'field', 
 'threadcount' : 1, 
 'hdrhistogram.percentiles' : '95,99'}

module.exports = {
	mode : mode,
	host : host,
	port : port,
	propPath : propPath,
	dbSetting : dbSetting,
	propAry : propAry,
	propDefault : propDefault
}
