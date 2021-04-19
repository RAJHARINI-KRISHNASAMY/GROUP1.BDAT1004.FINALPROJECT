#app.py
from pymongo import MongoClient
from flask import Flask, render_template,request, jsonify
import pymongo
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('Home.html')  
@app.route('/ques')
def ques():
    return render_template('Question.html')     #questions contains the fitgap analysis 
@app.route('/mileage')
def google_bar_chart():
    data=barchart1()          # barchart1() function does the query and returns the data in list of dict format
    return render_template('mileage_chart.html',data=data)

@app.route('/fuel_type') 
def google_pie_chart():       
    data=piechart1()          # piechart1() function does the query and returns the data in list of dict format
    return render_template('fuel_piechart.html', data=data)
@app.route('/cc')
def google_bar_chart1():
    data=barchart2()           # barchart2() function does the query and returns the data in list of dict format
    return render_template('cc_barchart.html', data=data)
@app.route('/SUV')
def google_bar_chart2():
    data=barchart3()            # barchart3() function does the query and returns the data in list of dict format
    return render_template('SUV_barchart.html', data=data)
@app.route('/Auto')
def google_pie_chart1():
    data=piechart2()            # piechart2() function does the query and returns the data in list of dict format
    return render_template('Auto_piechart.html', data=data)
@app.route('/api/cars/makers/all', methods=['GET'])# routing and type of http method to get all the data
def api_all():
    client = MongoClient("mongodb://BDAT1004:Password@cluster0-shard-00-00.gzlae.mongodb.net:27017,cluster0-shard-00-01.gzlae.mongodb.net:27017,cluster0-shard-00-02.gzlae.mongodb.net:27017/Mydatabase?ssl=true&replicaSet=atlas-qnmzak-shard-0&authSource=admin&retryWrites=true&w=majority")
    pipeline = [
        { '$match' : { 'manufacture_year' : {'$gte': 2009}}},
        {'$limit':100},
        {'$project' : { '_id' : 0 } }
    ]                                                   #selecting only the first 100 instance of cars manufactured after 2009
    data=list(client.Mydatabase.cars_csv.aggregate(pipeline))
    return jsonify(data)
@app.route('/api/cars/makers', methods=['GET'])         # routing and type of http get method with parameters
def api_id():       #for id parameter
    results = []
    client = MongoClient("mongodb://BDAT1004:Password@cluster0-shard-00-00.gzlae.mongodb.net:27017,cluster0-shard-00-01.gzlae.mongodb.net:27017,cluster0-shard-00-02.gzlae.mongodb.net:27017/Mydatabase?ssl=true&replicaSet=atlas-qnmzak-shard-0&authSource=admin&retryWrites=true&w=majority")
    pipeline = [
            { '$match' : { 'manufacture_year' : {'$gte': 2009}}},
            {'$limit':100},
            {'$project' : { '_id' : 0 } }
        ]
    data=list(client.Mydatabase.cars_csv.aggregate(pipeline))
    if 'maker' in request.args:     # for maker parameter
        id = request.args['maker']
        for maker in data:
            if maker['maker'] == id:
                results.append(maker)       
    elif 'id' in request.args:
        id=request.args['id']
        i=1
        for d in data:
            if i==int(id):
                return jsonify([d])
            i=i+1
    else:
        return "Error: No id field provided. Please specify a maker"
    return jsonify(results)

def piechart1():
    fuel_type=[]
    count=[]
    client = MongoClient("mongodb://BDAT1004:Password@cluster0-shard-00-00.gzlae.mongodb.net:27017,cluster0-shard-00-01.gzlae.mongodb.net:27017,cluster0-shard-00-02.gzlae.mongodb.net:27017/Mydatabase?ssl=true&replicaSet=atlas-qnmzak-shard-0&authSource=admin&retryWrites=true&w=majority")
    pipeline = [{'$sortByCount':'$fuel_type'}]      # above passed is the connection string to our collection in mongodb Atlas  
    data=list(client.Mydatabase.cars_csv.aggregate(pipeline)) # sortbycount groups and counts at the sametime
    for dt in data:
        for key in dt.keys():
            if(key=='_id'):
                fuel_type.append(dt[key])
            else:
                count.append(dt[key])
    combi=dict(zip(fuel_type, count)) # changing the query result type to {x axis:value,y axis:value}
    data={'Task':'percentage of car by fuel type'} # contains the legend for the chart
    data.update(combi)
    return data
def barchart1():
    client = MongoClient("mongodb://BDAT1004:Password@cluster0-shard-00-00.gzlae.mongodb.net:27017,cluster0-shard-00-01.gzlae.mongodb.net:27017,cluster0-shard-00-02.gzlae.mongodb.net:27017/Mydatabase?ssl=true&replicaSet=atlas-qnmzak-shard-0&authSource=admin&retryWrites=true&w=majority")

    pipeline = [
        { '$match' : { 'manufacture_year' : {'$gte': 2009}, 'pollution_test' : True } },
        {'$group' : {'_id' : '$maker', 'avg_mileage' : {'$avg' : '$mileage'},'avg_price':{'$avg':'$price'}}},
        { '$sort' : { 'avg_mileage' : -1 } },
        {'$limit':6}
    ] # -1 is decending and 1 is ascending ,default is 1
    data=list(client.Mydatabase.cars_csv.aggregate(pipeline))
    model=[]
    mileage=[]
    price=[]
    for dt in data: # extracting the x and y axis values
        for key in dt.keys():
            if(key=='_id'):
                model.append(dt[key])
            elif(key=='avg_mileage'):
                mileage.append(dt[key])
            else:
                price.append(dt[key])
    combi=dict(zip(model, mileage))
    data={'Task':'5 Makers by mileage'}
    data.update(combi)
    return data   
def barchart2():
    client = MongoClient("mongodb://BDAT1004:Password@cluster0-shard-00-00.gzlae.mongodb.net:27017,cluster0-shard-00-01.gzlae.mongodb.net:27017,cluster0-shard-00-02.gzlae.mongodb.net:27017/Mydatabase?ssl=true&replicaSet=atlas-qnmzak-shard-0&authSource=admin&retryWrites=true&w=majority")
    pipeline1 = [
        { '$match' : { 'manufacture_year' : {'$gte': 2009}, 'pollution_test' : True } },
        {'$group' : {'_id' : '$maker', 'avg_cc' : {'$avg' : '$engine_displacement'},'avg_price':{'$avg':'$price'}}},
        { '$sort' : { 'avg_cc' : -1 } },
        {'$limit':5}
    ]
    data1=list(client.Mydatabase.cars_csv.aggregate(pipeline1))
    m=[]
    cc=[]
    p=[]
    for d in data1:
        for key in d.keys():
            if(key=='_id'):
                m.append(d[key])
            elif(key=='avg_cc'):
                cc.append(d[key])
            else:
                p.append(d[key])
    data1={'Task':'5 Makers by cc'}
    cb1=dict(zip(m, cc))
    data1.update(cb1)
    return data1
def barchart3():
    client = MongoClient("mongodb://BDAT1004:Password@cluster0-shard-00-00.gzlae.mongodb.net:27017,cluster0-shard-00-01.gzlae.mongodb.net:27017,cluster0-shard-00-02.gzlae.mongodb.net:27017/Mydatabase?ssl=true&replicaSet=atlas-qnmzak-shard-0&authSource=admin&retryWrites=true&w=majority")
    pipeline2 = [
        { '$match' :{'car_type':'SUV'}},
        {'$sortByCount':'$maker'},
        { '$sort' : { 'count' : -1 } },
    ]# gets the maker and the no of SUV models released by the maker
    data2=list(client.Mydatabase.cars_csv.aggregate(pipeline2))
    m2=[]
    count=[]
    for d in data2:   
        for key in d.keys():
            if(key=='_id'):
                m2.append(d[key])
            else:
                count.append(d[key])
    data2={'Task':'MAKERS by SUV'}
    cb2=dict(zip(m2,count ))
    data2.update(cb2)
    return data2
def piechart2():
    client = MongoClient("mongodb://BDAT1004:Password@cluster0-shard-00-00.gzlae.mongodb.net:27017,cluster0-shard-00-01.gzlae.mongodb.net:27017,cluster0-shard-00-02.gzlae.mongodb.net:27017/Mydatabase?ssl=true&replicaSet=atlas-qnmzak-shard-0&authSource=admin&retryWrites=true&w=majority")
    pipeline4 = [
        {'$sortByCount':'$auto_facility'}
    ]
    data4=list(client.Mydatabase.cars_csv.aggregate(pipeline4))
    auto=[]
    count1=[]
    for d in data4:
        for key in d.keys():
            if(key=='_id'):
                auto.append(d[key])
            else:
                count1.append(d[key])
    data4={'Task':'percentage of car by gear type'}
    auto1=['Manual','Auto']
    cb4=dict(zip(auto1,count1 ))
    data4.update(cb4)   
    return data4
if __name__ == "__main__":
    app.run()