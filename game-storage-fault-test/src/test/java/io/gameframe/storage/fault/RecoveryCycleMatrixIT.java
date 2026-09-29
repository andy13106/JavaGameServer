package io.gameframe.storage.fault;

import com.mongodb.client.MongoClients;
import io.gameframe.storage.*;
import io.gameframe.storage.mongo.MongoDocumentStore;
import io.gameframe.storage.mysql.MySqlStore;
import io.gameframe.storage.redis.RedisStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_P35_CYCLES", matches=".+")
class RecoveryCycleMatrixIT {
    private static final int CYCLES = 2;
    private static String req(String n) { String v=System.getenv(n); assertNotNull(v,"missing "+n); assertFalse(v.isBlank(),"blank "+n); return v; }
    private static void docker(String action,String env) throws Exception {
        String c=req(env); assertTrue(c.matches("gameframe-p35-[a-z]+-20260920"),"unsafe cycle container");
        var p=new ProcessBuilder("docker",action,c).redirectErrorStream(true).start();
        assertTrue(p.waitFor(30,TimeUnit.SECONDS),"docker timeout");
        assertEquals(0,p.exitValue(),action+": "+new String(p.getInputStream().readAllBytes()));
    }
    private static void awaitMongo(String uri) throws Exception {
        long d=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        while(System.nanoTime()<d) { try(var c=MongoClients.create(uri)){ c.getDatabase("admin").runCommand(new org.bson.Document("ping",1)); return; } catch(RuntimeException e){Thread.sleep(300);} }
        fail("Mongo did not recover");
    }
    private static void awaitRedis(String uri) throws Exception {
        long d=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        while(System.nanoTime()<d) { try(var s=new RedisStore(uri,1,2)){ if("OK".equals(s.set("__p35_cycle_ping__","1",Duration.ofSeconds(2)).toCompletableFuture().get(2,TimeUnit.SECONDS))) return; } catch(Exception e){Thread.sleep(300);} }
        fail("Redis did not recover");
    }
    private static void awaitMysql(String c,String pw) throws Exception {
        long d=System.nanoTime()+TimeUnit.SECONDS.toNanos(120);
        while(System.nanoTime()<d) { var p=new ProcessBuilder("docker","exec",c,"mysqladmin","ping","-uroot","-p"+pw).redirectErrorStream(true).start(); if(p.waitFor(5,TimeUnit.SECONDS)&&p.exitValue()==0)return; Thread.sleep(500); }
        fail("MySQL did not recover");
    }

    @Test void mongoSurvivesRepeatedStopStartCycles() throws Exception {
        String uri=req("GAME_TEST_P35_CYCLES_MONGO_URI"), db="gameframe_p35_cycles_"+UUID.randomUUID().toString().replace("-","");
        try { for(int cycle=1;cycle<=CYCLES;cycle++) {
            StoreKey key = new StoreKey("cycle", "mongo-" + cycle);
            try(var s=new MongoDocumentStore(uri,db,1,2)) {
                assertEquals(WriteResult.Status.APPLIED,s.write(key,WriteCommand.create("cycle",Map.of("value","before-"+cycle))).toCompletableFuture().get(15,TimeUnit.SECONDS).status());
                docker("stop","GAME_TEST_P35_CYCLES_MONGO_CONTAINER");
                try { assertThrows(ExecutionException.class,()->s.load(key).toCompletableFuture().get(12,TimeUnit.SECONDS)); }
                finally { docker("start","GAME_TEST_P35_CYCLES_MONGO_CONTAINER"); awaitMongo(uri); }
            }
            try(var s=new MongoDocumentStore(uri,db,1,2)){ assertEquals(Optional.of(new Snapshot(1,Map.of("value","before-"+cycle))),s.load(key).toCompletableFuture().get(15,TimeUnit.SECONDS)); }
        }} finally { try(var c=MongoClients.create(uri)){c.getDatabase(db).drop();} }
    }

    @Test void redisSurvivesRepeatedStopStartCycles() throws Exception {
        String uri=req("GAME_TEST_P35_CYCLES_REDIS_URI"), key="gameframe:p35:cycles:"+UUID.randomUUID();
        try { for(int cycle=1;cycle<=CYCLES;cycle++) {
            try(var s=new RedisStore(uri,1,2)) {
                assertEquals("OK",s.set(key,"before-"+cycle,Duration.ofMinutes(1)).toCompletableFuture().get(10,TimeUnit.SECONDS));
                docker("stop","GAME_TEST_P35_CYCLES_REDIS_CONTAINER");
                try { assertThrows(ExecutionException.class,()->s.get(key).toCompletableFuture().get(12,TimeUnit.SECONDS)); }
                finally { docker("start","GAME_TEST_P35_CYCLES_REDIS_CONTAINER"); awaitRedis(uri); }
            }
            try(var s=new RedisStore(uri,1,2)){assertEquals("before-"+cycle,s.get(key).toCompletableFuture().get(10,TimeUnit.SECONDS));}
        }} finally { try(var s=new RedisStore(uri,1,2)){s.set(key,"",Duration.ofMillis(1)).toCompletableFuture().get(10,TimeUnit.SECONDS);}catch(Exception ignored){} }
    }

    @Test void mysqlSurvivesRepeatedStopStartCycles() throws Exception {
        String url=req("GAME_TEST_P35_CYCLES_MYSQL_URL"), user=req("GAME_TEST_P35_CYCLES_MYSQL_USER"), pw=req("GAME_TEST_P35_CYCLES_MYSQL_PASSWORD"), container=req("GAME_TEST_P35_CYCLES_MYSQL_CONTAINER"), table="cycles_"+UUID.randomUUID().toString().replace("-","");
        try { for(int cycle=1;cycle<=CYCLES;cycle++) {
            final int currentCycle = cycle;
            try(var s=new MySqlStore(url,user,pw,1,2)) {
                int n=s.execute(table,c->{try(var st=c.createStatement()){st.executeUpdate("CREATE TABLE IF NOT EXISTS "+table+" (id VARCHAR(32) PRIMARY KEY, value VARCHAR(64) NOT NULL)"); try(var u=c.prepareStatement("INSERT INTO "+table+" (id,value) VALUES (?,?) ON DUPLICATE KEY UPDATE value=VALUES(value)")){u.setString(1,"one");u.setString(2,"before-"+currentCycle);return u.executeUpdate();}}}).toCompletableFuture().get(15,TimeUnit.SECONDS); assertTrue(n>=1);
                docker("stop","GAME_TEST_P35_CYCLES_MYSQL_CONTAINER");
                try { assertThrows(ExecutionException.class,()->s.execute(table,c->1).toCompletableFuture().get(12,TimeUnit.SECONDS)); }
                finally { docker("start","GAME_TEST_P35_CYCLES_MYSQL_CONTAINER"); awaitMysql(container,pw); }
            }
            try(var s=new MySqlStore(url,user,pw,1,2)){assertEquals("before-"+cycle,s.execute(table,c->{try(var q=c.prepareStatement("SELECT value FROM "+table+" WHERE id=?")){q.setString(1,"one");try(var r=q.executeQuery()){return r.next()?r.getString(1):null;}}}).toCompletableFuture().get(15,TimeUnit.SECONDS));}
        }} finally {try(var c=DriverManager.getConnection(url,user,pw);var st=c.createStatement()){st.executeUpdate("DROP TABLE IF EXISTS "+table);}catch(Exception ignored){}}
    }
}