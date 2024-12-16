import random
import datetime as dt
from datetime import datetime, timedelta
from neo4j import GraphDatabase

DATABASE_SIZE = 100

def generate_data(size):
    # Opciones base para generar valores aleatorios
    brands = ["Sony", "Samsung", "LG", "Panasonic", "Philips", "Toshiba", "Sharp", "Vizio", "Hisense", "TCL"]
    addresses = ["Seoul", "Tokyo", "New York", "Los Angeles", "San Francisco", "Houston", "Miami", "Boston", "Chicago", "Dallas"]
    nations = ["South Korea", "Japan", "United States", "Canada", "Mexico", "Brazil", "Argentina", "Chile", "Colombia", "Peru"]
    regions = ["Asia", "North America", "South America"]
    dates = [f"2021-{str(month).zfill(2)}-{str(day).zfill(2)}" for month in range(1, 13) for day in [1, 29]]  # Genera fechas
    priorities = ["Urgent", "Normal", "Low", "High", "Medium", "Critical", "Non-Urgent", "Routine", "Immediate", "Deferred"]
    mktsegments = ["Electronics", "Home Appliances", "Mobile Devices", "Computers", "Audio", "Video", "Wearables", "Accessories", "Gaming", "Networking"]
    flags = ["Yes", "No"]

    # Generación dinámica de datos
    data = {
        "key": [str(10 + i) for i in range(size)],
        "brand": [random.choice(brands) for _ in range(size)],
        "address": [random.choice(addresses) for _ in range(size)],
        "nation": [random.choice(nations) for _ in range(size)],
        "region": [random.choice(regions) for _ in range(size)],
        "date": [random.choice(dates) for _ in range(size)],
        "priority": [random.choice(priorities) for _ in range(size)],
        "mktsegment": [random.choice(mktsegments) for _ in range(size)],
        "flag": [random.choice(flags) for _ in range(size)],
    }
    return data

# Ejemplo: Generar 30 elementos
data = generate_data(DATABASE_SIZE)

# DATABASE
def create_database(session):
    session.run("MATCH (n) DETACH DELETE n")  # drop all data in the database

    create_indexes(session)

    create_nodes(session, "Part", create_part_query)
    create_nodes(session, "Supplier", create_supp_query)
    create_nodes(session, "PartSupp", create_partsupp_query)
    create_nodes(session, "Nation", create_nation_query)
    create_nodes(session, "Region", create_region_query)
    create_nodes(session, "Order", create_order_query)
    create_nodes(session, "Customer", create_customer_query)
    create_nodes(session, "Lineitem", create_lineitem_query)

    create_relationships(session)


def create_indexes(session):
    # Índices
    session.run("CREATE INDEX IF NOT EXISTS FOR (li:Lineitem) ON (li.l_shipdate)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (ps:PartSupp) ON (ps.ps_supplycost)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (o:Order) ON (o.o_orderdate)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (p:Part) ON (p.p_size, p.p_type)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (r:Region) ON (r.r_name)")


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

# Query 2
def query2(session, size, type_filter, region):
    return session.run(
        """
        MATCH (p:Part)-[:HAS_PARTSUPP]->(ps:PartSupp)-[:SUPPLIED_BY]->(s:Supplier)-[:LOCATED_IN]->(n:Nation)-[:PART_OF]->(r:Region)
        WHERE p.p_size = $size 
          AND p.p_type CONTAINS $type_filter
          AND r.r_name = $region
        WITH p, ps, s, n, r, MIN(ps.ps_supplycost) AS min_supplycost
        WHERE ps.ps_supplycost = min_supplycost
        RETURN s.s_acctbal AS s_acctbal, 
               s.s_name AS s_name, 
               n.n_name AS n_name, 
               p.p_partkey AS p_partkey, 
               p.p_mfgr AS p_mfgr, 
               s.s_address AS s_address, 
               s.s_phone AS s_phone, 
               s.s_comment AS s_comment
        ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey
        """,
        {"size": size, "type_filter": type_filter, "region": region}
    )

# Query 3
def query3(session, segment, date1, date2):
    return session.run(
        """
        MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(l:Lineitem)
        WHERE c.c_mktsegment = $segment 
          AND o.o_orderdate < $date1
          AND l.l_shipdate > $date2
        WITH o.o_orderkey AS orderkey, 
             o.o_orderdate AS orderdate, 
             o.o_shippriority AS shippriority,
             SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
        RETURN orderkey, revenue, orderdate, shippriority
        ORDER BY revenue DESC, orderdate
        """,
        {"segment": segment, "date1": date1, "date2": date2}
    )

def query4(session, region, date):
    start_date = datetime.strptime(date, "%Y-%m-%d")
    end_date = start_date + timedelta(days=365)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    return session.run(
        """
        MATCH (r:Region)<-[:PART_OF]-(n:Nation)<-[:LOCATED_IN]-(s:Supplier)
              <-[:SUPPLIED_BY]-(ps:PartSupp)<-[:INCLUDES]-(l:Lineitem)<-[:CONTAINS]-(o:Order)-[:PLACED]->(c:Customer)
        WHERE r.r_name = $region
          AND o.o_orderdate >= date($start_date)
          AND o.o_orderdate < date($end_date)
        WITH n.n_name AS nation_name, 
             SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
        RETURN nation_name, revenue
        ORDER BY revenue DESC
        """,
        {"region": region, "start_date": start_date_str, "end_date": end_date_str}
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
        print("\nQuery 1:")
        result = query1(session, "2021-12-31")
        for record in result:
            print(record)

        print("\nQuery 2:")
        result = query2(session, 40, "Running", "South America")
        for record in result:
            print(record)
        
        print("\nQuery 3:")
        result = query3(session, "Electronics", "2021-06-01", "2021-03-01")
        for record in result:
            print(record)

        print("\nQuery 4:")
        result = query4(session, "Asia", "2021-06-06")
        for record in result:
            print(record)

            
    driver.close()

if __name__ == "__main__":
    main()