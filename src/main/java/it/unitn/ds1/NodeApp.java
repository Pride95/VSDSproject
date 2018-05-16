package it.unitn.ds1;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;

public class NodeApp {
  static private String remotePath = null; // Akka path of the bootstrapping peer

  public static class Join implements Serializable {
    int id;
    public Join(int id) {
      this.id = id;
    }
  }
  public static class RequestNodelist implements Serializable {}
  public static class Nodelist implements Serializable {
    Map<Integer, ActorRef> nodes;
    public Nodelist(Map<Integer, ActorRef> nodes) {
      this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes)); 
      }
    }
  
  public static class Node extends AbstractActor {
  
    // The table of all nodes in the system id->ref
    private Map<Integer, ActorRef> nodes = new HashMap<>();
    private String remotePath = null;
    private int id;

    /* -- Actor constructor --------------------------------------------------- */
    public Node(int id, String remotePath) {
      this.id = id;
      this.remotePath = remotePath;
    }

    static public Props props(int id, String remotePath) {
      return Props.create(Node.class, () -> new Node(id, remotePath));
    }

    public void preStart() {
      if (this.remotePath != null) {
        getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
      }
      nodes.put(this.id, getSelf());
    }

    private void onRequestNodelist(RequestNodelist message) {
        getSender().tell(new Nodelist(nodes), getSelf());
    }
    private void onNodelist(Nodelist message) {
      nodes.putAll(message.nodes);
      for (ActorRef n: nodes.values()) {
        n.tell(new Join(this.id), getSelf());
      }
    }
    private void onJoin(Join message) {
      int id = message.id;
      System.out.println("Node " + id + " joined");
      nodes.put(id, getSender());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .match(RequestNodelist.class, this::onRequestNodelist)
        .match(Nodelist.class, this::onNodelist)
        .match(Join.class, this::onJoin)
        .build();
    }
  }
  
  public static void main(String[] args) {

  // Load the configuration file
  Config config = ConfigFactory.load();
  int myId = config.getInt("nodeapp.id");
  String remotePath = null;

  if (config.hasPath("nodeapp.remote_ip")) {
    String remote_ip = config.getString("nodeapp.remote_ip");
    int remote_port = config.getInt("nodeapp.remote_port");
    // Starting with a bootstrapping node
    // The Akka path to the bootstrapping peer
    remotePath = "akka.tcp://mysystem@"+remote_ip+":"+remote_port+"/user/node";
    System.out.println("Starting node " + myId + "; bootstrapping node: " + remote_ip + ":"+ remote_port);
  }
  else {
    System.out.println("Starting disconnected node " + myId);
  }
  // Create the actor system
  final ActorSystem system = ActorSystem.create("mysystem", config);

  // Create a single node actor locally
  final ActorRef receiver = system.actorOf(
      Node.props(myId, remotePath),
      "node"      // actor name
      );
  }
}
