import jaydebeapi

url = 'jdbc:data:world:sql:dave:lahman-sabremetrics-dataset'

conn = jaydebeapi.connect('world.data.jdbc.DataWorldJdbcDriver',
                          ['jdbc:data:world:sql:dave:lahman-sabremetrics-dataset', 'dave', 'token'])
curs = conn.cursor()
curs.execute("select * from TABLE")
list = curs.fetchall()
print(list)

