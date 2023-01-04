CREATE (BI:Movie {title:'Bourne Identity'})
CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})
CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
CREATE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})
CREATE (Laurence:Person {name:'Laurence Fishburne', born:1961})
CREATE (Hugo:Person {name:'Hugo Weaving', born:1960})
CREATE (LillyW:Person {name:'Lilly Wachowski', born:1967})
CREATE (LanaW:Person {name:'Lana Wachowski', born:1965})
CREATE (JoelS:Person {name:'Joel Silver', born:1952})
CREATE (MissionImpossible:Movie {title:'Mission Impossible', released:1996, tagline:'Expect the impossible'})
CREATE (Tom:Person {name:'Tom Cruise', born:1962})
CREATE (Jon:Person {name:'Jon Voight', born:1938})
CREATE (Brian:Person {name:'Brian De Palma', born:1940})
CREATE
(Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix),
(Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrix),
(Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrix),
(Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrix),
(LillyW)-[:DIRECTED]->(TheMatrix),
(LanaW)-[:DIRECTED]->(TheMatrix),
(JoelS)-[:PRODUCED]->(TheMatrix),
(Tom)-[:ACTED_IN {roles:['Ethan']}]->(MissionImpossible),
(Jon)-[:ACTED_IN {roles:['Jim']}]->(MissionImpossible),
(Brian)-[:DIRECTED]->(MissionImpossible);


