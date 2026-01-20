// Conecta usuários com mensagens enviadas em comum DENTRO DE 15 SEGUNDOS
MATCH (u1:User)-[:SENT]->(m1:Mensagem)-[:HAS_TEXT]->(t:Texto)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)

WHERE u1.id < u2.id AND abs(duration.between(m1.date_message, m2.date_message).seconds) <= 15

WITH u1, u2, count(*) AS shared_count_10s

MERGE (u1)-[r:RAPID_SHARE]-(u2)
SET r.weight = shared_count_10s;

// Conecta usuários que compartilham mensagens virais em tempo curto
MATCH (u1:User)-[:SENT]->(m1:Mensagem)-[:HAS_TEXT]->(t:Texto)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)
WHERE u1.id < u2.id
  AND m1.viral = true
  AND m2.viral = true
  AND abs(duration.between(m1.date_message, m2.date_message).seconds) <= 15

WITH u1, u2, 
     count(*) as viral_amplifications,
     avg(abs(duration.between(m1.date_message, m2.date_message).seconds)) as avg_viral_delay

MERGE (u1)-[r:VIRAL_AMPLIFIER]-(u2)
SET r.weight = viral_amplifications,
    r.avg_delay = avg_viral_delay;

// Conecta usuários que compartilham mensagens com desinformação em tempo curto
MATCH (u1:User)-[:SENT]->(m1:Mensagem)-[:HAS_TEXT]->(t:Texto)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)
WHERE u1.id < u2.id
  AND m1.score_misinformation > 0.8
  AND m2.score_misinformation > 0.8
  AND abs(duration.between(m1.date_message, m2.date_message).seconds) <= 15

WITH u1, u2, 
     count(*) as misinfo_amplifications,
     avg(abs(duration.between(m1.date_message, m2.date_message).seconds)) as avg_viral_delay

MERGE (u1)-[r:MISINFORMATION_AMPLIFIER]-(u2)
SET r.weight = misinfo_amplifications,
    r.avg_delay = avg_viral_delay;

// Conecta usuários que compartilham o mesmo texto na mesma hora do dia (em dias diferentes ou não)
MATCH (t:Texto)<-[:HAS_TEXT]-(m1:Mensagem)<-[:SENT]-(u1:User)
MATCH (t)<-[:HAS_TEXT]-(m2:Mensagem)<-[:SENT]-(u2:User)

WHERE u1.id < u2.id
  AND m1.date_message.hour = m2.date_message.hour

WITH u1, u2,
     count(*) AS hourly_shared_count

WHERE hourly_shared_count >= 3

MERGE (u1)-[r:HOURLY_SHARED]-(u2)
SET r.weight = hourly_shared_count;

// Cria score de sincronicidade entre usuários que compartilharam mensagens muito rapidamente
MATCH (u:User)-[r:RAPID_SHARE]-(target:User)
WITH u, sum(r.weight) as rapid_weight_sum, count(target) as rapid_partners
SET u.synchronicity_score = rapid_weight_sum * log(rapid_partners + 1);

// Cria arestas de semelhança de sincronicidade
CREATE INDEX user_sync_score IF NOT EXISTS FOR (u:User) ON (u.synchronicity_score);

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

// Cria scores de compartilhamentos
MATCH (u:User)
// Score de compartilhamentos geral
OPTIONAL MATCH (u)-[r1:SHARES]-()
WITH u, sum(r1.weight) as raw_shares
// Score de compartilhamentos virais
OPTIONAL MATCH (u)-[r2:VIRAL_SHARES]-()
WITH u, raw_shares, sum(r2.weight) as raw_viral
// Score de compartilhamento de desinformação
OPTIONAL MATCH (u)-[r3:SHARES_MISINFORMATION]-()
WITH u, raw_shares, raw_viral, sum(r3.weight) as raw_misinfo
// Cria scores normalizados com log
SET u.shares_score = log10(1.0 + coalesce(raw_shares, 0))
SET u.viral_score = log10(1.0 + coalesce(raw_viral, 0))
SET u.misinfo_score = log10(1.0 + coalesce(raw_misinfo, 0));

// Cria aresta similaridade de compartilhamentos gerais
MATCH (u:User)
WHERE u.shares_score > 0
WITH collect(u) as activeSharers

UNWIND activeSharers as u1
UNWIND activeSharers as u2
WITH u1, u2
WHERE u1.id < u2.id
  AND abs(u1.shares_score - u2.shares_score) / u1.shares_score < 0.05

MERGE (u1)-[r:SHARES_SIMILAR]-(u2)
SET r.weight = 1.0 - (abs(u1.shares_score - u2.shares_score) / u1.shares_score);

// Cria aresta similaridade de compartilhamentos virais
MATCH (u:User)
WHERE u.viral_score > 0
WITH collect(u) as viralSharers

UNWIND viralSharers as u1
UNWIND viralSharers as u2
WITH u1, u2
WHERE u1.id < u2.id
  AND abs(u1.viral_score - u2.viral_score) / u1.viral_score < 0.05

MERGE (u1)-[r:VIRAL_SIMILAR]-(u2)
SET r.weight = 1.0 - (abs(u1.viral_score - u2.viral_score) / u1.viral_score);

// Cria aresta similaridade de compartilhamentos com desinformação
MATCH (u:User)
WHERE u.misinfo_score > 0
WITH collect(u) as misinfoSpreaders

UNWIND misinfoSpreaders as u1
UNWIND misinfoSpreaders as u2
WITH u1, u2
WHERE u1.id < u2.id
  AND abs(u1.misinfo_score - u2.misinfo_score) / u1.misinfo_score < 0.05

MERGE (u1)-[r:MISINFO_SIMILAR]-(u2)
SET r.weight = 1.0 - (abs(u1.misinfo_score - u2.misinfo_score) / u1.misinfo_score);

// Cria score de singularidade de mensagens
MATCH (u:User)-[:SENT]->(m:Mensagem)-[:HAS_TEXT]->(t:Texto)
WITH u, count(m) as total_msgs, count(DISTINCT t) as unique_texts

MATCH (t)<-[:HAS_TEXT]-(:Mensagem)
WITH u, total_msgs, unique_texts, 
     count { (t)<-[:HAS_TEXT]-(:Mensagem) } AS total_text_usage

SET u.content_originality = toFloat(unique_texts) / total_msgs,
    u.content_uniqueness = toFloat(unique_texts) / total_text_usage;

// Cria score de diversidade de conexões
MATCH (u:User)-[:SHARES]-(other:User)
WITH u, collect(DISTINCT other.id_most_active_group) as partner_groups

MATCH (u)-[:SHARES]-(other:User)
WITH u, partner_groups, 
     count(other) as total_partners,
     size(partner_groups) as unique_groups

SET u.network_diversity = toFloat(unique_groups) / total_partners;