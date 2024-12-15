import random
import datetime as dt
from neo4j import GraphDatabase

# DATA
data = {
    "key": ["11", "12", "13", "14", "15", "16", "17", "18", "19", "20"],
    "brand": ["Sony", "Samsung", "LG", "Panasonic", "Philips", "Toshiba", "Sharp", "Vizio", "Hisense", "TCL"],
    "address": ["Seoul", "Tokyo", "New York", "Los Angeles", "San Francisco", "Houston", "Miami", "Boston", "Chicago", "Dallas"],
    "nation": ["South Korea", "Japan", "United States", "Canada", "Mexico", "Brazil", "Argentina", "Chile", "Colombia", "Peru"],
    "region": ["Asia", "Asia", "North America", "North America", "North America", "South America", "South America", "South America", "South America", "South America"],
    "date": ["2021-01-01", "2021-02-02", "2021-03-03", "2021-04-04", "2021-05-05", "2021-06-06", "2021-07-07", "2021-08-08", "2021-09-09", "2021-10-10"],
    "priority": ["Urgent", "Normal", "Low", "High", "Medium", "Critical", "Non-Urgent", "Routine", "Immediate", "Deferred"],
    "mktsegment": ["Electronics", "Home Appliances", "Mobile Devices", "Computers", "Audio", "Video", "Wearables", "Accessories", "Gaming", "Networking"],
    "flag": ["Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No"]
}

# DATABASE
def create_database(session):
    session.run("MATCH (n) DETACH DELETE n")  # drop all data in the database

    create_nodes(session, "Part", create_part_query)
    create_nodes(session, "Supplier", create_supp_query)
    create_nodes(session, "PartSupp", create_partsupp_query)
    create_nodes(session, "Nation", create_nation_query)
    create_nodes(session, "Region", create_region_query)
    create_nodes(session, "Order", create_order_query)
    create_nodes(session, "Customer", create_customer_query)
    create_nodes(session, "Lineitem", create_lineitem_query)

    create_relationships(session)

# GENERIC NODE CREATION
def create_nodes(session, label, query_func):
    for i in range(len(data["key"])):
        session.run(query_func(i))

# RELATIONSHIPS
def create_relationships(session):
    for i in range(len(data["key"])):
        session.run(create_part_partsupp_rel_query(i))
        session.run(create_partsupp_supp_rel_query(i))
        session.run(create_supp_nation_rel_query(i))
        session.run(create_nation_region_rel_query(i))
        session.run(create_customer_nation_rel_query(i))
        session.run(create_customer_order_rel_query(i))
        session.run(create_order_lineitem_rel_query(i))
        session.run(create_lineitem_partsupp_rel_query(i))

# RELATIONSHIP QUERIES
def create_part_partsupp_rel_query(i):
    return f"MATCH (p:Part{{p_partkey: {data['key'][i]}}}), (ps:PartSupp{{ps_partkey: {data['key'][i]}}}) CREATE (p)-[:HAS_PARTSUPP]->(ps)"

def create_partsupp_supp_rel_query(i):
    return f"MATCH (ps:PartSupp{{ps_suppkey: {data['key'][i]}}}), (s:Supplier{{s_suppkey: {data['key'][i]}}}) CREATE (ps)-[:SUPPLIED_BY]->(s)"

def create_supp_nation_rel_query(i):
    return f"MATCH (s:Supplier{{s_suppkey: {data['key'][i]}}}), (n:Nation{{n_nationkey: {data['key'][i]}}}) CREATE (s)-[:LOCATED_IN]->(n)"

def create_nation_region_rel_query(i):
    return f"MATCH (n:Nation{{n_nationkey: {data['key'][i]}}}), (r:Region{{r_regionkey: {data['key'][i]}}}) CREATE (n)-[:PART_OF]->(r)"

def create_customer_nation_rel_query(i):
    return f"MATCH (c:Customer{{c_custkey: {data['key'][i]}}}), (n:Nation{{n_nationkey: {data['key'][i]}}}) CREATE (c)-[:RESIDES_IN]->(n)"

def create_customer_order_rel_query(i):
    return f"MATCH (c:Customer{{c_custkey: {data['key'][i]}}}), (o:Order{{o_orderkey: {data['key'][i]}}}) CREATE (c)-[:PLACED]->(o)"

def create_order_lineitem_rel_query(i):
    return f"MATCH (o:Order{{o_orderkey: {data['key'][i]}}}), (l:Lineitem{{l_linenumber: {data['key'][i]}}}) CREATE (o)-[:CONTAINS]->(l)"

def create_lineitem_partsupp_rel_query(i):
    return f"MATCH (l:Lineitem{{l_linenumber: {data['key'][i]}}}), (ps:PartSupp{{ps_partkey: {data['key'][i]}}}) CREATE (l)-[:INCLUDES]->(ps)"

# QUERIES
def create_part_query(i):
    return f"CREATE (part{data['key'][i]}: Part{{p_partkey: {data['key'][i]}, p_name: 'Partkey{data['key'][i]}', p_mfgr: 'ABCDEFG', p_brand: '{data['brand'][i]}', p_type: 'Running', p_size: {random.randint(38, 45)}, p_container: 'Container{data['key'][i]}', p_retailprice: {float(random.randint(1000, 5000) / 100)}, p_comment: 'OK'}})"

def create_supp_query(i):
    return f"CREATE (supp{data['key'][i]}: Supplier{{s_suppkey: {data['key'][i]}, s_name: 'Supplier{data['key'][i]}', s_address: '{data['address'][i]}', s_phone: {random.randint(600000000, 699999999)}, s_acctbal: {random.random()}, s_comment: 'OK'}})"

def create_partsupp_query(i):
    return f"CREATE (partsupp{data['key'][i]}: PartSupp{{ps_partkey: {data['key'][i]}, ps_suppkey: {data['key'][i]}, ps_availqty: {random.randint(100, 500)}, ps_supplycost: {float(random.randint(100, 500) / 100)}, ps_comment: 'OK'}})"

def create_nation_query(i):
    if i < len(data["nation"]):
        return f"CREATE (nation{data['key'][i]}: Nation{{n_nationkey: {data['key'][i]}, n_name: '{data['nation'][i]}', n_comment: 'OK'}})"
    return ""

def create_region_query(i):
    if i < len(data["region"]):
        return f"CREATE (region{data['key'][i]}: Region{{r_regionkey: {data['key'][i]}, r_name: '{data['region'][i]}', r_comment: 'OK'}})"
    return ""

def create_order_query(i):
    return f"CREATE (order{data['key'][i]}: Order{{o_orderkey: {data['key'][i]}, o_orderstatus: 'OK', o_totalprice: {random.randint(0, 1000)}, o_orderdate: '{random.choice(data['date'])}', o_orderpriority: '{random.choice(data['priority'])}', o_clerk: 'Louis', o_shippriority: '{random.choice(data['priority'])}', o_comment: 'OK'}})"

def create_customer_query(i):
    return f"CREATE (customer{data['key'][i]}: Customer{{c_custkey: {data['key'][i]}, c_name: 'Supplier{data['key'][i]}', c_address: '{data['address'][i]}', c_phone: {random.randint(600000000, 699999999)}, c_acctbal: {random.random()}, c_mktsegment: '{random.choice(data['mktsegment'])}', s_comment: 'OK'}})"

def create_lineitem_query(i):
    return f"CREATE (lineitem{data['key'][i]}: Lineitem{{l_linenumber: {data['key'][i]}, l_quantity: {random.randint(0, 100)}, l_extendedprice: {random.randint(0, 200)}, l_discount: {random.randint(0, 99)}, l_tax: {random.randint(0, 20)}, l_returnflag: '{random.choice(data['flag'])}', l_linestatus: '{random.choice(data['flag'])}', l_shipdate: '{random.choice(data['date'])}', l_commitdate: '{random.choice(data['date'])}', l_receiptdate: '{random.choice(data['date'])}', l_shipinstruct: 'Ok{data['key'][i]}', l_shipmode: 'Ok{data['key'][i]}', l_comment: 'OK'}})"

# Query 1
def query1(session, date):
    return session.run(
        "MATCH (li: Lineitem) "
        "WHERE li.l_shipdate <= $date "
        "RETURN li.l_returnflag AS l_returnflag, li.l_linestatus AS l_linestatus, sum(li.l_quantity) AS sum_qty, "
        "sum(li.l_extendedprice) AS sum_base_price, sum(li.l_extendedprice * (1 - li.l_discount)) AS sum_disc_price, "
        "sum(li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) AS sum_charge, avg(li.l_quantity) AS avg_qty, "
        "avg(li.l_extendedprice) AS avg_price, AVG(li.l_discount) AS avg_disc, COUNT(*) AS count_order "
        "ORDER BY li.l_returnflag, li.l_linestatus",
        {"date": date}
    )

# MAIN
def main():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "12345678"
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        create_database(session)
        print("Database created successfully")
        
        # Call query1
        result = query1(session, "2021-12-31")
        for record in result:
            print(record)
            
    driver.close()

if __name__ == "__main__":
    main()