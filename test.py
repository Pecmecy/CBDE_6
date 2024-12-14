from neo4j import GraphDatabase

# Initialize the database driver
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Create a session
session = driver.session()

# Functions to interact with the database
def create_person(name, age):
    query = """
    CREATE (p:Person {name: $name, age: $age})
    """
    session.run(query, name=name, age=age)

def get_people():
    query = """
    MATCH (p:Person)
    RETURN p.name AS name, p.age AS age
    """
    result = session.run(query)
    for record in result:
        print(f"Name: {record['name']}, Age: {record['age']}")

def create_friendship(person1, person2):
    query = """
    MATCH (a:Person {name: $person1}), (b:Person {name: $person2})
    CREATE (a)-[:FRIEND]->(b)
    """
    session.run(query, person1=person1, person2=person2)

# Example usage
create_person("Alice", 30)
create_person("Bob", 25)
create_friendship("Alice", "Bob")
get_people()

# Close session and driver
session.close()
driver.close()
