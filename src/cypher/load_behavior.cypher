// Conecta usuários com mensagens enviadas em comum DENTRO DE 15 SEGUNDOS
MATCH (u1:User)-[:SENT]->(m1:Mensagem)-[:HAS_TEXT]->(t:Texto)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)

WHERE u1.id < u2.id AND abs(duration.between(m1.date_message, m2.date_message).seconds) <= 15

WITH u1, u2, count(*) AS shared_count_10s

MERGE (u1)-[r:RAPID_SHARE]-(u2)
SET r.weight = shared_count_10s;

// Cria score de sincronicidade entre usuários que compartilharam mensagens muito rapidamente
MATCH (u:User)-[r:RAPID_SHARE]-(target:User)
WITH u, sum(r.weight) as rapid_weight_sum, count(target) as rapid_partners
SET u.synchronicity_score = rapid_weight_sum * log(rapid_partners + 1);

// Cria score de flood
MATCH (u:User)-[:SENT]->(m:Mensagem)
WITH u, count(m) as total_msgs, count(distinct m.id_group_anonymous) as distinct_groups

MATCH (u)-[:SENT]->(m)-[:HAS_TEXT]->(t:Texto)
WITH u, total_msgs, distinct_groups, count(distinct t) as unique_texts

WITH u, total_msgs, 
     (toFloat(total_msgs) / CASE WHEN unique_texts = 0 THEN 1 ELSE toFloat(unique_texts) END) as repetition_ratio
WHERE total_msgs > 5

SET u.flooding_score = log10(total_msgs) * repetition_ratio;

// Cria índices
CREATE INDEX user_flood_score IF NOT EXISTS FOR (u:User) ON (u.flooding_score);
CREATE INDEX user_sync_score IF NOT EXISTS FOR (u:User) ON (u.synchronicity_score);

// Cria arestas de semelhança de flooding
MATCH (u:User)
WHERE u.flooding_score > 1.0 
WITH collect(u) as suspiciousUsers

UNWIND suspiciousUsers as u1
UNWIND suspiciousUsers as u2
WITH u1, u2
WHERE u1.id < u2.id
  AND abs(u1.flooding_score - u2.flooding_score) / u1.flooding_score < 0.05

MERGE (u1)-[r:FLOOD_SIMILAR]-(u2)
SET r.weight = 1.0 - (abs(u1.flooding_score - u2.flooding_score) / u1.flooding_score);

// Cria arestas de semelhança de sincronicidade
MATCH (u:User)
WHERE u.synchronicity_score > 0
WITH collect(u) as suspiciousSync

UNWIND suspiciousSync as u1
UNWIND suspiciousSync as u2
WITH u1, u2
WHERE u1.id < u2.id
  AND abs(u1.synchronicity_score - u2.synchronicity_score) / u1.synchronicity_score < 0.05

MERGE (u1)-[r:SYNC_SIMILAR]-(u2)
SET r.weight = 1.0 - (abs(u1.synchronicity_score - u2.synchronicity_score) / u1.synchronicity_score);