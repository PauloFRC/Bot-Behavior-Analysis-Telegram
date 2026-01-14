// Apaga todos os nós antes de carregar novos dados
MATCH (m:Mensagem) DETACH DELETE m;
MATCH (u:User) DETACH DELETE u;
MATCH (t:Texto) DETACH DELETE t;

// Cria índices para melhorar a performance das consultas
CREATE INDEX texto_index IF NOT EXISTS FOR (t:Texto) ON (t.texto);
CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.id);
CREATE INDEX IF NOT EXISTS FOR (m:Mensagem) ON (m.neo4j_id);

// Carrega dados do CSV e cria nós de mensagens e textos
// Lembrar que o arquivo CSV deve estar localizado na pasta 'import' do Neo4j
LOAD CSV WITH HEADERS FROM 'file:///telegram_tratado.csv' AS row
MERGE (t:Texto {texto: row.texto_limpo})
CREATE (m:Mensagem {
    neo4j_id: randomUUID(),
    date_message: datetime(replace(row.date_message, " ", "T")),
    id_message: row.id_message,
    id_member_anonymous: row.id_member_anonymous,
    id_group_anonymous: row.id_group_anonymous,
    media_type: row.media_type,
    has_media: toBoolean(row.has_media),
    score_sentiment: toFloat(row.score_sentiment),
    score_misinformation: toFloat(row.score_misinformation),
    message_type: row.message_type,
    message_count: toInteger(row.msg_count),
    viral: toBoolean(row.viral)
})
MERGE (m)-[:HAS_TEXT]->(t);

// Cria nós de usuários e os relaciona com as mensagens
MATCH (m:Mensagem)
WITH DISTINCT m.id_member_anonymous AS user_id
MERGE (u:User {id: user_id});

MATCH (m:Mensagem)
MATCH (u:User {id: m.id_member_anonymous})
MERGE (u)-[:SENT]->(m);

// Assinala usuários ao seu grupo mais ativo para auxiliar em análises
MATCH (u:User)-[:SENT]->(m:Mensagem)
WITH u, m.id_group_anonymous AS group_id, count(*) AS msg_count
WITH u, collect({group: group_id, count: msg_count}) AS groups
WITH u,
     reduce(best = {group: null, count: 0}, g IN groups |
         CASE WHEN g.count > best.count THEN g ELSE best END
     ) AS top_group
SET u.id_most_active_group = top_group.group;


// Conecta usuários com mensagens enviadas em comum
MATCH (t:Texto)<-[:HAS_TEXT]-(m1:Mensagem)<-[:SENT]-(u1:User)
MATCH (t)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)
WHERE u1.id < u2.id
WITH u1, u2, count(t) AS shared_count
MERGE (u1)-[r:SHARES]-(u2)
SET r.weight = shared_count;

// Conecta usuários com mensagens VIRAIS enviadas em comum
MATCH (t:Texto)<-[:HAS_TEXT]-(m1:Mensagem)<-[:SENT]-(u1:User)
MATCH (t)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)
WHERE u1.id < u2.id 
  AND m1.viral = true 
  AND m2.viral = true
WITH u1, u2, count(t) AS viral_shared_count
MERGE (u1)-[r:VIRAL_SHARES]-(u2)
SET r.weight = viral_shared_count;

// Conecta usuários com mensagens com maior grau de DESINFORMAÇÃO enviadas em comum
MATCH (t:Texto)<-[:HAS_TEXT]-(m1:Mensagem)<-[:SENT]-(u1:User)
MATCH (t)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)
WHERE u1.id < u2.id
  AND m1.score_misinformation > 0.8
  AND m2.score_misinformation > 0.8
WITH u1, u2, count(t) AS shared_misinfo_count
MERGE (u1)-[r:SHARES_MISINFORMATION]-(u2)
SET r.weight = shared_misinfo_count;

// Cria os nós de Grupo e conecta usuarios caso tenha mensagem pertecente ao grupo
MATCH (m:Mensagem) WITH DISTINCT m.id_group_anonymous AS group_id MERGE (g:Grupo {id: group_id});

MATCH (u:User)-[:SENT]->(m:Mensagem)
MATCH (g:Grupo {id: m.id_group_anonymous})
MERGE (u)-[:PARTE_DE]->(g);