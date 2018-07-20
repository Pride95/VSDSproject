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
import akka.actor.PoisonPill;
import scala.concurrent.duration.Duration;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NodeApp {
	
	private static class actorData implements Serializable{
		ActorRef ref;
		genericMessage lastMessage;
		long timer;
		public actorData(ActorRef ref, genericMessage lastMessage, long timer) {
			this.ref = ref;
			this.lastMessage = lastMessage;
			this.timer = timer;
		}
	}
	
	public static class genericMessage implements Serializable{}
	
	public static class joinRequest extends genericMessage implements Serializable{
		ActorRef ref;
		int idPeer;
		public joinRequest(ActorRef ref, int id){
			this.ref = ref;
			this.idPeer = id;
		}
	}
	
	public static class joinResponse extends genericMessage implements Serializable{
		int IDactor;
		public joinResponse(int IDactor) {
			this.IDactor = IDactor;
		}
	}
	
	public static class changeView extends genericMessage implements Serializable{
		int newIDView;
		Map<Integer, actorData> newView;
		public changeView(int newIDView, Map<Integer, actorData> newView) {
			this.newIDView = newIDView;
			this.newView = new HashMap<>();
			for(int key : newView.keySet()){
				this.newView.put(key, new actorData(newView.get(key).ref, null, 0));
			}
		}
	}
	
	public static class FLUSH extends genericMessage implements Serializable{
		int ID;
		int IDview;
		int IDnewView;
		public FLUSH(int ID, int idview, int IDnewView) {
			this.ID = ID;
			this.IDview = idview;
			this.IDnewView = IDnewView;
		}
	}
	
	public static class cacheMessage extends genericMessage implements Serializable{
		//il senderid e l'id chi chi invia effettivamente il messaggii
		int newsenderID;
		//il creatorId invece e l'id di chi ha creato il messaggio che sta venendo mandato in forma di cache
		int creatorID;
		int messageID;
		int IDview;
		public cacheMessage(int newsenderID, int creatorID, int messageID, int IDview) {
			this.newsenderID = newsenderID;
			this.creatorID = creatorID;
			this.messageID = messageID;
			this.IDview = IDview;
		}
	}
	
	public static class normalMessage extends genericMessage implements Serializable{
		//id di chi lo manda
		int senderID;
		int messageID;
		int IDview;
		public normalMessage(int senderID, int messageID, int v) {
			this.senderID = senderID;
			this.messageID = messageID;
			this.IDview = v;
		}
	}

	public static class heart extends genericMessage implements Serializable{
		int idSender;
		public heart(int id) {
			this.idSender = id;
		}
	}

        
        
        
	public static class Node extends AbstractActor {
		private Map<Integer, actorData> view = new HashMap<>();
		private int IDview;
		
		private Map<Integer, actorData> newView = null;
		
		private int newIDview;
		
		private Map<Integer, genericMessage> tempFLUSH = new HashMap<>();
		
		private Queue<genericMessage> codaMessaggi = new LinkedList<>();
				
		private boolean onJoin = false;
		private boolean onChange = false;
		
		private String remotePath = null;
		
		private String filename = "";
		private BufferedWriter out;
		
		private int id;
		private int privateId;
		
		private int IDactor = 1;
		
		private int messageID = 0;
		private int maxMessagge = 20;
		
		Random rand = new Random();
		
		private Queue<ActorRef> toJoinPeer = new  LinkedList<>();
		
		private boolean crashNormal = false;
		private boolean crashCache = false;
		private boolean crashFlush = false;
		private boolean crashChange = false;
		private boolean isCrash = false;
		
		private boolean joinOnJoin = false;
		private boolean joinOnJoinexetuted = false;
		
		private boolean joinOnChange = false;
		private boolean joinOnChangeExecuted = false;
		
		private boolean fileReset = false;
		private boolean isFinish = false;
		
		/* -- Actor constructor --------------------------------------------------- */
		public Node(String remotePath, int privateID, boolean[] conditions) {
			this.remotePath = remotePath;
			this.privateId = privateID;
			/*
			in order we have
			0 : crash normal
			1 : crash cache
			2 : crash flush
			3 : crash change

			4 : join peer (for coord.)
			5 : join on join (for coord.)
			6 : join on change
			*/
			crashNormal = conditions[0];
			crashCache = conditions[1];
			crashFlush = conditions[2];
			crashChange = conditions[3];
			
			joinOnJoin = conditions[4];
			joinOnChange = conditions[5];
			
		}

		static public Props props(int id, String remotePath, boolean[] conditions) {
			return Props.create(Node.class, () -> new Node(remotePath, id, conditions));
		}
		
		@Override
		public void preStart() {
			System.out.println("START PEER");
			if (this.remotePath != null) {
				onJoin = true;
				System.out.println("attempt to enter with contacting " + remotePath);
				getContext().actorSelection(remotePath).tell(new joinRequest( getSelf(), privateId ) , getSelf());
				System.out.println("request sended");
				this.maxMessagge = 20;
			}
			else{
				id = 0;
				System.out.println("I AM THE COORDINATOR");
				view.put(id, new actorData(getSelf(), null, 0));
				System.out.println("this is the initial view: " + view.toString() );
				getSelf().tell(new heart(id), getSelf());
				filename = filename + "output" + this.id + ".txt";
				log("Initializing peer " + this.id + " \n");
				this.maxMessagge = 70;
			}
		}
		
		private void onJoinRequest (joinRequest mess){
			System.out.println("join request recived from: " + mess.ref + " with privateID : " + mess.idPeer);
			
			if(mess.idPeer < 0){
				System.out.println("the requested peer is a peer with negative id, i put aside it and insert it in key point of the protocol");
				toJoinPeer.add(mess.ref);
			}
			else{
				//check if we are in change view
				if(newView != null){
					System.out.println("attempt of inserting a new peer during a chenge view, so i update the new view");
					Map<Integer, actorData> tempview = new HashMap<>();
					newIDview = newIDview+1;
					for(int key : newView.keySet()){
						tempview.put(key, new actorData(newView.get(key).ref, null, 0));
					}
					tempview.put(IDactor, new actorData(mess.ref, null, 0));
					newView = tempview;
				}
				else{
					newIDview = IDview+1;
					newView = new HashMap<>();
					for(int key : view.keySet()){
						newView.put(key, new actorData(view.get(key).ref, null, 0));
					}
					newView.put(IDactor, new actorData(mess.ref, null, 0));
				}
				System.out.println("send invitation to: " + IDactor);
				mess.ref.tell(new joinResponse(IDactor), getSelf());
				
				for (int key : newView.keySet()){
					System.out.println("send changeview messagge to: " + key + " --- " + newView.get(key).ref.toString());
					newView.get(key).ref.tell(new changeView(newIDview, newView), getSelf());
				}
				IDactor++;
				if(joinOnJoin && !toJoinPeer.isEmpty() && ! joinOnJoinexetuted){
					ActorRef refNewPeer = toJoinPeer.poll();
					joinOnJoinexetuted = true;
					onJoinRequest(new joinRequest(refNewPeer, 1));
				}
			}
		}
		
		private void onJoinResponse(joinResponse mess){
			this.id = mess.IDactor;
			System.out.println("I recive a join response and my new ID is: " + mess.IDactor);
			filename = filename + "output" + this.id + ".txt";
			log("Initializing peer " + this.id + " \n");
		}
		
		private void onChangeView (changeView mess){
			if(crashChange && id == 1 && IDview ==1){
				isCrash = true;
				System.out.println("sono il peer 1 e crasho appena ricevuto il messaggio di change view e sono nella view 1");
				context().stop(getSelf());
			}
			else{
				this.onChange = true;
				this.newIDview = mess.newIDView;
				this.newView = mess.newView;
				//here there are possible FLUSH comming before i start change view
				if(tempFLUSH != null){
					System.out.println("i tempFLUSH non sono vuoti");
					for(int key : tempFLUSH.keySet()){
						//in the if i check if the flush in the tempFLUSH is correct for the new View
						if(newView.containsKey(key) && ((FLUSH)tempFLUSH.get(key)).IDnewView == newIDview){
							newView.get(key).lastMessage = tempFLUSH.get(key);
						}
					}
					tempFLUSH = null;
				}

				System.out.println("must change the view and it is:" + printView(newView) );
				if(!onJoin){
					int numMessaggi = 0;
					System.out.println(">>> SEND CACHE");
					System.out.println("old view is: " + printView(view));

					for(int key : view.keySet()){
						if(view.get(key).lastMessage != null){

							normalMessage last = (normalMessage)view.get(key).lastMessage;

							System.out.println("sending messagge " + last.messageID +
									" comming from " + last.senderID + 
									" from the view " + last.IDview);
							cacheMessage c = new cacheMessage(id, last.senderID, last.messageID, last.IDview);
							if(!isCrash){
								multicast(newView, c);
								numMessaggi++;
							}
						}
					}

					System.out.println("are send " + numMessaggi + " cache message");
					System.out.println(">>> END CACHE");

					if(id == 0){
						for(int key : newView.keySet()){
							newView.get(key).timer = System.currentTimeMillis();
						}
					}
				}
				else{
					System.out.println("I'm new so i need to send only FLUSH");
				}


				if(!isCrash){
					if(id != 0 || newView.size()==1){
						System.out.println(">>> SEND FLUSH");
						if(onJoin){
							multicast(newView, new FLUSH(id, newIDview - 1, newIDview));
						}
						else{
							multicast(newView, new FLUSH(id, IDview, newIDview));
						}
						System.out.println(">>> END FLUSH");

						if(onJoin){
							System.out.println("i'm new i'm starting sending heartbeat");
							newView.get(0).ref.tell(new heart(id), getSelf());
							getSelf().tell(new heart(id), getSelf());
						}
					}
					else{}
				}
			}
			if(id == 0 && joinOnChange && !joinOnChangeExecuted && !toJoinPeer.isEmpty() && newIDview > 2){
				ActorRef refNewPeer = toJoinPeer.poll();
				joinOnChangeExecuted = true;
				onJoinRequest(new joinRequest(refNewPeer, 1));
			}
		}
		
		private void onFLUSH (FLUSH mess){
			System.out.println("arrived FLUSH message from: " + mess.ID + " sendend in the view: " + mess.IDview);
			if(newView == null && !onChange){
				System.out.println("arrived a flush before i was in the change view");
				if (mess.IDview >= IDview){
					if(tempFLUSH == null){
						tempFLUSH = new HashMap<>();
					}
					tempFLUSH.put(mess.ID, mess);
				}
			}
            else if(newView != null && onChange && mess.IDnewView == newIDview){
				if(mess.ID == 0){
					System.out.println("stable view because it arrived a FLUSH from the coordinator");
					installView();
				}
				else{
					if(newView.containsKey(mess.ID)){
						this.newView.get(mess.ID).lastMessage = mess;
						//ciclo su tutti e se tutti sono in flush io installo la view

						boolean install = true;
						int numFlush = 0;
						for (int key : newView.keySet()){

							if (this.newView.get(key).lastMessage != null ){
								//System.out.println("vedo il " + key + " con il messaggio : " + this.newView.get(key).lastMessage.toString());
								if (! (this.newView.get(key).lastMessage instanceof FLUSH)){
									install = false;
								}
								else{
									numFlush++;
								}
							}
							else{
								//System.out.println("il last messaggio da " + key + " era NULL");
								install = false;
							}
						}

						if(install){
							System.out.println("view stabile ora installo");
							installView();
						}
						else if(id == 0 && numFlush == (newView.size()-1) ){
							System.out.println(">>> I'm the coordinator: START FLUSH");
							multicast(newView, new FLUSH(id, IDview, newIDview));
							System.out.println(">>> I'm the coordinator: END FLUSH");
						}
					}
					else{
						System.out.println("!!!ignorato un messaggio di FLUSH proveniente da " + mess.ID + " inviato nella view " 
								+ mess.IDview );
					}
				}
			}
			else if(mess.IDnewView < newIDview){
				System.out.println("!!!!!!!!!!!!!mi è arrivato un messaggio di flush non per la mia nuova view lo ignoro per " + mess.IDnewView +
						"deve essere per " + newIDview );
			}
			else if(mess.IDnewView > newIDview){
				System.out.println("!!!!!!!!!!!mi è arrivato un messaggio di flush non per la mia nuova view lo tengo da parte per " + mess.IDnewView +
						"deve essere per " + newIDview );
				if(tempFLUSH == null){
					tempFLUSH = new HashMap<>();
				}
				tempFLUSH.put(mess.ID, mess);
			}
			else{
				System.out.println(">>>>>>>>> CRITICAL ERROR <<<<<<<<<");
			}
		}
		
		private void onNormalMessage(normalMessage mess){
			System.out.println("arrived messagge from: " + mess.senderID + " send in view:" + mess.IDview + 
					" i'm in the view " + IDview + " with next view " + newIDview + " messagge id " + mess.messageID);
			if(!onChange){
				if(view.containsKey(mess.senderID)){
					genericMessage last = view.get(mess.senderID).lastMessage;
					if(last!= null){
						deliver((normalMessage)last);
					}
					else{
						System.out.println(">>>> last message was null");
					}
					view.get(mess.senderID).lastMessage = mess;
				}
				else{
					System.out.println(">>> discarded messagge from " + mess.senderID + " send in the view " 
							+ mess.IDview + " we are in the view " + IDview);
				}
			}
			else{
				if(codaMessaggi == null){
					codaMessaggi = new LinkedList<>();
				}
				//codaMessaggi is a queue that have all the normal message that are arrived during change view, so 
				//we need to process them during the installation of the new view
				System.out.println("arrived normal message during change view from : " + mess.messageID + " insert in the queue");
				codaMessaggi.add(mess);
			}
			if(!onChange && messageID < maxMessagge && mess.senderID == id){
				//send a new message
				normalMessage m = new normalMessage(id, messageID, IDview);
				messageID++;
				System.out.println("in the onNormalMessage start new message " + m.messageID + " from view " + IDview);
				multicast(view, m);
			}
			else if(messageID == maxMessagge && id != 0){
				isFinish = true;
			}
		}
		
		private void onCacheMessage(cacheMessage mess){
			if(!onJoin){
				System.out.println("arrived cache message from " + mess.newsenderID + " from the peer " + mess.creatorID 
						+ " send in the view " + mess.IDview);
				if(view.get(mess.creatorID) != null){
					normalMessage last = (normalMessage)view.get(mess.creatorID).lastMessage;
					if(last != null){
						if(mess.messageID > last.messageID){
							System.out.println("messagge of cache arrived is useful");
							deliver(last);
							view.get(mess.creatorID).lastMessage = new normalMessage(mess.creatorID, mess.messageID, mess.IDview);
						}
						else{
							System.out.println("message cache is useless, last messagge id: " + last.messageID 
									+ " cache mess id: " + mess.messageID);
						}
					}
				}
			}
		}
		
		private void onHeart(heart m){
			if(id == 0){
				System.out.println("arrivato heartbit da " + m.idSender);
				if(onChange){
					if(newView.containsKey(m.idSender)){
						newView.get(m.idSender).timer = System.currentTimeMillis();
						timeCheck(newView);
					}
					else{
						System.out.println("arrived a heartbeat from a peer defined dead");
					}
				}
				else{
					if(view.containsKey(m.idSender)){
						view.get(m.idSender).timer = System.currentTimeMillis();
						timeCheck(view);
					}
					else{
						System.out.println("arrived a heartbeat from a peer defined dead");
					}
				}
			}
			if(m.idSender == id && !isFinish){
				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
					Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
				}

				System.out.println("restart heartbit");
				if(newView != null){
					newView.get(0).ref.tell(new heart(id), getSelf());
				}
				else if(view != null){
					view.get(0).ref.tell(new heart(id), getSelf());
				}
				if(id != 0){
					getSelf().tell(new heart(id), getSelf());
				}
			}
		}
				
		private void installView(){
			Queue<genericMessage> messaggiNuovaView = new LinkedList<>();
			for (int key : newView.keySet()){
				newView.get(key).lastMessage = null;
				if(id == 0) {
					newView.get(key).timer = System.currentTimeMillis();
				}
				else{
					newView.get(key).timer = 0;
				}
			}
			if(!onJoin){
				for(int key : view.keySet()){
					if(view.get(key).lastMessage != null){
						deliver((normalMessage)view.get(key).lastMessage);
					}
				}	
				if(codaMessaggi != null){
					while(!codaMessaggi.isEmpty()){
						normalMessage m = (normalMessage) codaMessaggi.poll();
						if(m.IDview < newIDview){
							System.out.println("extract from the queue message : " + m.messageID + " send from : " + m.senderID);
							if(view.containsKey(m.senderID)){
								if(view.get(m.senderID).lastMessage != null){
									normalMessage last = (normalMessage) view.get(m.senderID).lastMessage;
									if(last.messageID < m.messageID){
										System.out.println("the message is correct and now i deliver it");
										deliver(m);
									}
								}
							}
							else{
							}
						}
						else{
							messaggiNuovaView.add(m);
						}
					}
					codaMessaggi = null;
				}
			}
			System.out.println("<"+id+"> INSTALL VIEW " + newIDview + "--" + printView(newView));
			//write log
			log(" "+id+" install view " + newIDview + "  " + printPartecipantView(newView) + " \n");
			
			this.view = this.newView;
			this.newView = null;
			this.IDview = this.newIDview;
			this.onChange = false;
			this.onJoin = false;
			//here there are the message that i must deliver after installing the view
			while(!messaggiNuovaView.isEmpty()){
				normalMessage m = (normalMessage) messaggiNuovaView.poll();
				if(view.containsKey(m.senderID)){
					normalMessage last = (normalMessage) view.get(m.senderID).lastMessage;
					if(last != null){
						deliver(last);
					}
					view.get(m.senderID).lastMessage = m;
				}
			}
			//metto un controllo cosi ne invia solo un numero fisso
			if(messageID < maxMessagge){
				normalMessage m = new normalMessage(id, messageID, IDview);
				messageID++;
				System.out.println("in the installView start sending messagge " + m.messageID + " from the view " + IDview);
				multicast(view, m);
			}
			else if(messageID >= maxMessagge && id != 0){
				isFinish = true;
			}
		}
		
		private void timeCheck(Map<Integer, actorData> currentview){
			long current = System.currentTimeMillis();
			int timeout = 8000; 
			Map<Integer, actorData> tempView = new HashMap<>();
			
			for(int key : currentview.keySet()){
				tempView.put(key, new actorData(currentview.get(key).ref, null, 0));
			}
			
			boolean crash = false;
			boolean sendnewview = true;
			
			for(int key : currentview.keySet()){
				if(currentview.get(key).timer != 0 && key != 0 ){
					long difference = current - currentview.get(key).timer;
					if(difference > timeout){
						crash = true;
						System.out.println(">>>>> CRASH !!!!!!  peer " + key + " not responding from " + difference);
						//send pioson pill to kill the peer
						currentview.get(key).ref.tell(PoisonPill.getInstance(), getSelf());
						tempView.remove(key);
					}
				}
				else{
					currentview.get(key).timer = current;
				}
			}
			
			if(crash){
				if(newView!=null){
					newIDview = newIDview+1;
				}else{
					newIDview = IDview+1;
				}
				
				if(newView != null){
					//control for not send duplicate change view
					boolean firstcheck = newView.keySet().containsAll(tempView.keySet());
					boolean secondcheck = tempView.keySet().containsAll(newView.keySet());
					
					if(firstcheck && secondcheck){
						sendnewview = false;
						System.out.println("i was to send a duplicate change view, i avoided it");
					}
					else if(firstcheck && !secondcheck){
						Map<Integer, actorData> newPeers = new HashMap<>();
						for(int key : newView.keySet()){
							newPeers.put(key, new actorData(newView.get(key).ref, null, 0));
						}
						for(int key : view.keySet()){
							newPeers.remove(key);
						}
						System.out.println("<<<<<sono nel caso in cui sto cambiando view ma trovo un peer crasiato e quindi devo investigare se c'e un peer "
								+ "aggiunto che potrei perdere");
						if(!newPeers.isEmpty()){
							System.out.println("<<<<ho tolto dalla nuova view tutti i peer vecchi e quindi questi rimanenti sono quelli in piu");
							for(int key : newPeers.keySet()){
								tempView.put(key, newPeers.get(key));
							}
						}
						else{
							System.out.println("<<<<falso allarme di perdita di peer");
						}
					}
				}
				if(sendnewview){
					newView = new HashMap<>();
					newView.putAll(tempView);
					for (int key : newView.keySet()){
						newView.get(key).lastMessage = null;
						newView.get(key).timer = 0;
					}
					System.out.println(">>>> CRASH localized and the new view is: " + printView(newView));
					for (int key : newView.keySet()){
						System.out.println("send the change view message to : " + key + " --- " + newView.get(key).ref.toString());
						newView.get(key).ref.tell(new changeView(newIDview, newView), getSelf());
					}
				}
			}
		}
		
		private String printView(Map<Integer, actorData> m){
			String word = "";
			word = word + "{ \n" ;
			for(int key : m.keySet()){
				word = word + key + " = \n \t" ;
				word = word + "ref : " + m.get(key).ref + " \n \t" ;
				if(m.get(key).lastMessage != null){
					word = word + "last message = messageID : " + ((normalMessage)m.get(key).lastMessage).messageID +
							" senderID : " + ((normalMessage)m.get(key).lastMessage).senderID +
							" \n \t" ;
				}else{
					word = word + "last message = NULL \n \t";
				}
				word = word + "timer : " + m.get(key).timer + " \n";
			}
			word = word + "} \n" ;
			return word;
		}
		
		private String printPartecipantView(Map<Integer, actorData> m){
			String word = "";
			for(int key : m.keySet()){
				word = word + key + "," ;
			}
			return word;
		}
		
		private void deliver(normalMessage m){
			System.out.println("<"+id+"> Deliver new messagge with id : " + m.messageID + " send from: " 
					+ m.senderID + " send in the view : " + m.IDview + " we are in the view " + IDview);
			log(" "+id+" deliver multicast " + m.messageID + " from " + m.senderID + " within " + IDview + "\n");
		}
		
		private void multicast(Map<Integer, actorData> v, genericMessage m){
			int timeSleep = rand.nextInt(1000) + 1000 ;
			if(m instanceof normalMessage){
				System.out.println("<"+id+"> SEND MULTICAST " + ((normalMessage)m).messageID 
						+ " IN THE VIEW " + ((normalMessage)m).IDview);
				//log write
				log(" "+id+" send multicast " + ((normalMessage) m).messageID + " within " + IDview + " \n");
			}
			try {
				Thread.sleep(timeSleep);
			} catch (InterruptedException ex) {
				Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
			}
			for(int key : v.keySet()){
				v.get(key).ref.tell(m, getSelf());
				if(crashNormal && id != 0 && messageID == 10 && (m instanceof normalMessage)){
					System.out.println("i am the peer " + id + " and i will crash now after sending the messagge " + messageID);
					context().stop(getSelf());
					isCrash = true;
					break;
				}
				
				if(crashCache && id != 0 && (m instanceof cacheMessage)){
					System.out.println("sono il peer 1 e sto il messaggio di cache ma fingo il crash");
					context().stop(getSelf());
					isCrash = true;
					break;
				}
				
				
				if(crashFlush && id != 0 && IDview == 1 && (m instanceof FLUSH)){
					
					System.out.println("sono il peer 1 e sto mandando un messaggio di flush e fingo il crach");
					context().stop(getSelf());
					isCrash = true;
					break;
				}
			}
		}

		private void log(String content){
			try (BufferedWriter out = new BufferedWriter(new FileWriter(filename, fileReset))){
				out.append(content);
				fileReset = true;
			} catch (IOException ex) {
				Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		
		@Override
		public Receive createReceive() {
			return receiveBuilder()
					.match(joinRequest.class, this::onJoinRequest)
					.match(joinResponse.class, this::onJoinResponse)
					.match(changeView.class, this::onChangeView)
					.match(FLUSH.class, this::onFLUSH)
					.match(normalMessage.class, this::onNormalMessage)
                    .match(cacheMessage.class, this::onCacheMessage)
					.match(heart.class, this::onHeart)
					.build();
		}
	}

	public static void main(String[] args) {

		// Load the configuration file
		Config config = ConfigFactory.load();
		int myId = config.getInt("nodeapp.id");
		String remotePath = null;
		boolean addJoinPeer = false;
		boolean[] conditions = new boolean[6];
		/*
		in order we have
		0 : crash normal
		1 : crash cache
		2 : crash flush
		3 : crash change
		
		
		4 : join on join (for coord.)
		5 : join on change
		*/

		if (config.hasPath("nodeapp.remote_ip")) {
			String remote_ip = config.getString("nodeapp.remote_ip");
			int remote_port = config.getInt("nodeapp.remote_port");
			// Starting with a bootstrapping node
			// The Akka path to the bootstrapping peer
			remotePath = "akka.tcp://mysystem@" + remote_ip + ":" + remote_port + "/user/node";
			System.out.println("Starting node " + myId + "; bootstrapping node: " + remote_ip + ":" + remote_port);
		} else {
			System.out.println("Starting disconnected node " + myId);
		}
		
		if(config.hasPath("nodeapp.crashNormal")){
			conditions[0] = config.getBoolean("nodeapp.crashNormal");
		}
		else{
			conditions[0] = false;
		}
		
		if(config.hasPath("nodeapp.crashCache")){
			conditions[1] = config.getBoolean("nodeapp.crashCache");
		}else{
			conditions[1] = false;
		}
		
		if(config.hasPath("nodeapp.crashFlush")){
			conditions[2] = config.getBoolean("nodeapp.crashFlush");
		}else{
			conditions[2] = false;
		}
		
		if(config.hasPath("nodeapp.crashChange")){
			conditions[3] = config.getBoolean("nodeapp.crashChange");
		}else{
			conditions[3] = false;
		}
		
		
		if(config.hasPath("nodeapp.joinOnJoin")){
			conditions[4] = config.getBoolean("nodeapp.joinOnJoin");
		}else{
			conditions[4] = false;
		}
		
		if(config.hasPath("nodeapp.joinOnChange")){
			conditions[5] = config.getBoolean("nodeapp.joinOnChange");
		}else{
			conditions[5] = false;
		}
		
		
		
		if(myId == 0){
			System.out.println("Starting coordinator system");
		}
		// Create the actor system
		final ActorSystem system = ActorSystem.create("mysystem", config);

		
		// Create a single node actor locally
		if(myId == 0){
			final ActorRef receiver = system.actorOf(
					Node.props(myId, null, conditions),
					"node" // actor name
			);
		}
		else{
			final ActorRef receiver = system.actorOf(
					Node.props(myId, remotePath, conditions),
					"node" // actor name
			);
		}
		
		
		if(myId == 0 && addJoinPeer){
			system.actorOf(Node.props(myId - 1, remotePath, conditions),
				"peer1"
			);
			system.actorOf(Node.props(myId - 2, remotePath, conditions),
				"peer2"
			);
		}else if(myId == 0){
			System.out.println("i am the coordinator but i dont start any more peer");
		}
	}
}
