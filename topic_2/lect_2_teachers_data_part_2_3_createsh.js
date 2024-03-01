db = connect( 'mongodb://localhost/thirddb' );

var data = JSON.parse(fs.readFileSync('/home/jovyan/__DATA/IBDT_Spring_2024/topic_2/teachers-20240301.json', 'utf8'));

db.teachers.insert(data);

printjson( db.teachers.findOne() );