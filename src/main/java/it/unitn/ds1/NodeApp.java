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

	static private String remotePath = null; // Akka path of the bootstrapping peer

	
	//classe per incapsulare tutti i dati di un attore inerenti ad una view
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
			//this.newView = newView;
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
	
	//questo messaggio se lo manda il cordinatore a se stesso cosi rimane sveglio 
	//per controllare sempre i timers.
	public static class heart extends genericMessage implements Serializable{
		int idSender;

		public heart(int id) {
			this.idSender = id;
		}
		
	}

        
        
        
	public static class Node extends AbstractActor {

		// The table of all nodes in the system id->ref
		
		//questa è la view
		//private Map<Integer, ActorRef> nodes = new HashMap<>();
		
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
		
		//questo è il proprio id di ciascun nodo
		private int id;
		private int privateId;
		
		//questo è l'id assegnato agli attori che si uniscono ed è uso esclusivo del coordinator
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
		
		private boolean joinOnChange = true;
		private boolean joinOnChangeExecuted = false;
		
		private boolean fileReset = false;
		
		private boolean isFinish = false;
		
		/* -- Actor constructor --------------------------------------------------- */
		public Node(String remotePath, int privateID) {
			this.remotePath = remotePath;
			this.privateId = privateID;
		}

		static public Props props(int id, String remotePath) {
			return Props.create(Node.class, () -> new Node(remotePath, id));
		}
		
		@Override
		public void preStart() {
			System.out.println("AVVIO PEER");
			if (this.remotePath != null) {
				//sono qui solo se non sono il coordinator e sono appena entrato
				
				//mi metto nello stato di sto aspettando di entrare
				onJoin = true;
				//invio la richiesta di entrare
				System.out.println("cerco di entrare e chiedo a " + remotePath);
				getContext().actorSelection(remotePath).tell(new joinRequest( getSelf(), privateId ) , getSelf());
				System.out.println("ho mandato il messaggio");
				this.maxMessagge = 20;
			}
			else{
				//il coordinatore si riconosce e si autosetta il proprio id
				//e si inserisce nella view.
				id = 0;
				System.out.println("sono il Coordinator");
				view.put(id, new actorData(getSelf(), null, 0));
				System.out.println("e questa e' la view: " + view.toString() );
				getSelf().tell(new heart(id), getSelf());
				
				filename = filename + "output" + this.id + ".txt";
				
				log("Initializing peer " + this.id + " \n");
				this.maxMessagge = 70;
			}
		}
		
		private void onJoinRequest (joinRequest mess){
			System.out.println("mi e' arrivata una richiesta da: " + mess.ref + " con privateID : " + mess.idPeer);
			
			if(mess.idPeer < 0){
				System.out.println("sta cercando di entrare un peer con id negativo, lo metto da parte e lo faro entrare "
						+ "nei punti cruciali del protocollo");
				toJoinPeer.add(mess.ref);
			}
			else{
				//controllo che sto cambiando gia view, se è cosi
				//estendo la nuova view e non la vecchia
				if(newView != null){
					System.out.println("sta avvenendo una entrata mentre stiamo cambiando view, quindi estendo la nuova view");
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

				//pulire tutti i last message perche non li devo mandare in giro.
				
				/*
				for (int key : newView.keySet()){
					newView.get(key).lastMessage = null;
					newView.get(key).timer = 0;
				}*/

				//mando un messaggio a chi vuole entrare
				System.out.println("sto mandando il messaggio a: " + IDactor);
				mess.ref.tell(new joinResponse(IDactor), getSelf());

				//ciclo for per mandare a tutti nella nuova view il messaggio di changeview !
				//mettere questo ciclo in una funzione e sara nominata multicast

				for (int key : newView.keySet()){
					System.out.println("mando il messaggio di changeview a: " + key + " --- " + newView.get(key).ref.toString());
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
			System.out.println("mi è arrivata la risposta di join e sono: " + mess.IDactor);
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


				if(tempFLUSH != null){
					System.out.println("i tempFLUSH non sono vuoti");
					for(int key : tempFLUSH.keySet()){
						if(newView.containsKey(key)){
							newView.get(key).lastMessage = tempFLUSH.get(key);
						}
					}
					tempFLUSH = null;
				}

				System.out.println("devo cambiare view, la nuova view e' :" + printView(newView) );
				if(!onJoin){

					//INVIO CACHE
					int numMessaggi = 0;

					System.out.println("INVIO CACHE");
					System.out.println("la vecchia view e' : " + printView(view));

					for(int key : view.keySet()){
						if(view.get(key).lastMessage != null){

							normalMessage last = (normalMessage)view.get(key).lastMessage;

							System.out.println("inizio invio messaggio " + last.messageID +
									" inviato dal peer " + last.senderID + 
									" appartenente alla view " + last.IDview);

							cacheMessage c = new cacheMessage(id, last.senderID, last.messageID, last.IDview);

							if(!isCrash){
								multicast(newView, c);
								numMessaggi++;
							}
						}
					}

					System.out.println("sono stati inviati " + numMessaggi + " messaggi di cache ");
					System.out.println("FINE CACHE");

					if(id == 0){
						for(int key : newView.keySet()){
							newView.get(key).timer = System.currentTimeMillis();
						}
					}

				}
				else{
					System.out.println("sono nuovo e devo mandare solo i FLUSH");
				}


				if(!isCrash){
					System.out.println("comincio a mandare i messaggi di flush");
					//problema se sono nuovo è possibile che mando in giro messaggi che arrivano da una view 0
					//il che mi puo portare problemi quando devo salvare i flush nei flush temporanei
					//per cui metto che li ho inviari nella view prima di quella nuova
					
					if(id != 0 || newView.size()==1){
						if(onJoin){
							multicast(newView, new FLUSH(id, newIDview - 1, newIDview));
						}
						else{
							//se invece non sono nuovo non c'è nessun problema
							multicast(newView, new FLUSH(id, IDview, newIDview));
						}
						System.out.println("finito di mandare i flush");

						if(onJoin){
							System.out.println("sono nuovo e comincio gli heartbit");
							newView.get(0).ref.tell(new heart(id), getSelf());
							getSelf().tell(new heart(id), getSelf());
						}
					}
					else{
						//sono il coordinator e quindi li mando dopo
					}
				}
			}
			
			if(id == 0 && joinOnChange && !joinOnChangeExecuted && !toJoinPeer.isEmpty() && newIDview > 2){
				//inserisco un nuovo peer quando sto facendo un change view
				
				ActorRef refNewPeer = toJoinPeer.poll();
				joinOnChangeExecuted = true;
				onJoinRequest(new joinRequest(refNewPeer, 1));
				
			}
			
		}
		
		
		private void onFLUSH (FLUSH mess){
			
			System.out.println("arrivato un messaggio di FLUSH da: " + mess.ID + " che e' partito dalla view : " + mess.IDview);
			
			if(newView == null && !onChange){
				System.out.println("mi è arrivato un FLUSH prima che fossi in onChangeView");
				if (mess.IDview >= IDview){
				
					if(tempFLUSH == null){
						tempFLUSH = new HashMap<>();
					}
					tempFLUSH.put(mess.ID, mess);
					
				}
			}
            else if(newView != null && onChange && mess.IDnewView == newIDview){
			
				//qui si inserisce il controllo che se il sender del flush è il coordinator la view viene 
				//installata senza aspettare gli altri FLUSH
				
				
				if(mess.ID == 0){
					System.out.println("view stabile per arrivo FLUSH da coordinator ora installo");
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
							//multicast flush
							System.out.println("Sono il coordinator: comuncio a mandare i flush");
							multicast(newView, new FLUSH(id, IDview, newIDview));
							System.out.println("Sono il coordinator: finito di mandare i flush");
						}
					}
					else{
						//siamo qui perche ci è arrivato un FLUSH da un peer non presente nella newview
						//quindi probabilmente considerato morto per errore
						//ignoriamo il FLUSH e mandiamo il messaggio di terminazione

						//qui semplicemente ignoriamo i messasggi
						//tanto gli abbiamo detto di morire gia prima nel timecheck

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
				//mi è arrivato un flush per una view futura e quindi dovrei tenerlo da parte
				System.out.println("!!!!!!!!!!!mi è arrivato un messaggio di flush non per la mia nuova view lo tengo da parte per " + mess.IDnewView +
						"deve essere per " + newIDview );
				if(tempFLUSH == null){
					tempFLUSH = new HashMap<>();
				}
				tempFLUSH.put(mess.ID, mess);
			}
			else{
				System.out.println("!!!sono arrivato in un punto del protocollo morto controllo!!!!");
			}
		}
		
		private void onNormalMessage(normalMessage mess){
            
			System.out.println("arrivato messaggio da : " + mess.senderID + " partito nella view " + mess.IDview + 
					" sono nella view " + IDview + " con prossima view " + newIDview + " con messagge id " + mess.messageID);
			
			//faccio lo stesso controllo che se per caso mi arriva un messaggio da un peer 
			//dichiarato morto per sbaglio lo ignoro
			
			if(!onChange){
				
				if(view.containsKey(mess.senderID)){
					genericMessage last = view.get(mess.senderID).lastMessage;

					if(last!= null){
						deliver((normalMessage)last);
					}
					else{
						System.out.println("!!!!!!!!!!!!il last message era null");
					}

					view.get(mess.senderID).lastMessage = mess;
				}
				else{
					//è arrivato un messaggio da un peer che non è presente nella view
					//lo ignoro perche gli è stato inviato il messaggio di morte gia prima
					System.out.println("!!!ingorato messaggio proveniente da " + mess.senderID + " mandato nella view " 
							+ mess.IDview + " siamo nella view " + IDview);
				}
				
				
			}
			else{
				
				if(codaMessaggi == null){
					codaMessaggi = new LinkedList<>();
				}
				
				System.out.println("e' arrivato un messaggio durante la change view da : " + mess.messageID + " e lo metto in coda");
				codaMessaggi.add(mess);
				
			}

			if(!onChange && messageID < maxMessagge && mess.senderID == id){
				
				/*if(id == 1 && messageID == 20){
					System.out.println("fingo di morire");
					try {
						Thread.sleep(10000);
					} catch (InterruptedException ex) {
						Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
					}
				}*/
				
				normalMessage m = new normalMessage(id, messageID, IDview);
				messageID++;
				
				System.out.println("nella onNormalMessage inizio invio messaggio " + m.messageID + " appartenente alla view " + IDview);

				multicast(view, m);
			}
			else if(messageID == maxMessagge && id != 0){
				//context().stop(getSelf());
				isFinish = true;
			}
                 
            
		}
		
		private void onCacheMessage(cacheMessage mess){
			
			if(!onJoin){
				System.out.println("arrivato cache message da " + mess.newsenderID + " inerente al peer " + mess.creatorID 
						+ " partito dalla view " + mess.IDview);
				
				if(view.get(mess.creatorID) != null){


					normalMessage last = (normalMessage)view.get(mess.creatorID).lastMessage;

					if(last != null){
						if(mess.messageID > last.messageID){
							System.out.println("messaggio cache arrivato e' un nuovo messaggio quindi mi è utile");
							deliver(last);
							view.get(mess.creatorID).lastMessage = new normalMessage(mess.creatorID, mess.messageID, mess.IDview);
						}
						else{
							System.out.println("il messaggio di cache non mi e' utile, last messagge id: " + last.messageID 
									+ " cache mess id: " + mess.messageID);
						}
					}
				}
				/*
				if(id == 0){
					if(onChange){
						newView.get(mess.newsenderID).timer = System.currentTimeMillis();
						timeCheck(newView);

					}else{
						view.get(mess.newsenderID).timer = System.currentTimeMillis();
						timeCheck(view);
					}
				}
				*/

			}

		}
		
		
		private void onHeart(heart m){
			
			
			if(id == 0){
				
				//eseguire i check per far si che se uno è dichiarato morto per sbaglio
				//non faccia andare tutto in null pointer
				System.out.println("arrivato heartbit da " + m.idSender);
				if(onChange){
					if(newView.containsKey(m.idSender)){
						newView.get(m.idSender).timer = System.currentTimeMillis();
						timeCheck(newView);
					}
					else{
						//arrivato un heartbeat da un peer considerato morto
						System.out.println("arrivato un heartbeat da un peer considerato morto");
						
					}
				}
				else{
					if(view.containsKey(m.idSender)){
						view.get(m.idSender).timer = System.currentTimeMillis();
						timeCheck(view);
					}
					else{
						//arrivato un heartbeat da un peer considerato morto
						System.out.println("arrivato un heartbeat da un peer considerato morto");
					}
				}
			}
			
			
			if(m.idSender == id && !isFinish){
				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
					Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
				}

				System.out.println("rinivio heartbit");
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
			//pulisco la nuova view dai messaggi di FLUSH salvati
			
			Queue<genericMessage> messaggiNuovaView = new LinkedList<>();
			
			for (int key : newView.keySet()){
				newView.get(key).lastMessage = null;
				//newView.get(key).timer = 0;
				//la modifico che inserisco il timer di quando installo
				if(id == 0) {
					newView.get(key).timer = System.currentTimeMillis();
				}
				else{
					newView.get(key).timer = 0;
				}
			}
			
			//quando installo la view posso tranquillamente fare il delivery di tutti i messaggi inerenti 
			//alla vecchia view che ho ancora salvato in memoria
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
						
							System.out.println("dalla coda estraggo il mess : " + m.messageID + " inviato da : " + m.senderID);

							if(view.containsKey(m.senderID)){

								if(view.get(m.senderID).lastMessage != null){
									normalMessage last = (normalMessage) view.get(m.senderID).lastMessage;

									if(last.messageID < m.messageID){
										System.out.println("il messaggio estratto è corretto e quindi lo delivero");
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
			
			
			//invio messaggi appartenenti alla view appena installata ma che mi sono arrivati
			//prima che la installassi
			
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
				//qui cominciamo a inviare oppure a riprendere

				normalMessage m = new normalMessage(id, messageID, IDview);
				messageID++;
				
				System.out.println("nella installView inizio invio messaggio " + m.messageID + " appartenente alla view " + IDview);
				
				multicast(view, m);
			}
			else if(messageID >= maxMessagge && id != 0){
				//context().stop(getSelf());
				isFinish = true;
			}
		}
		
		private void timeCheck(Map<Integer, actorData> currentview){
			
			//inserire lista dei peer eliminati cosi da  non eliminarli di nuovo
			
			long current = System.currentTimeMillis();
			
			int timeout = 8000; //sono millisecond
								//abbiamo incrementato il timer per il problema che grazie agli
								//heartbit il tutto diventa più lento.
			
			Map<Integer, actorData> tempView = new HashMap<>();
			
			
			for(int key : currentview.keySet()){
				//tempView.putAll(currentview);
				tempView.put(key, new actorData(currentview.get(key).ref, null, 0));
			}
			
			boolean crash = false;
			
			boolean sendnewview = true;
			
			for(int key : currentview.keySet()){
				if(currentview.get(key).timer != 0 && key != 0 ){
					long difference = current - currentview.get(key).timer;
					
					
					if(difference > timeout){
						crash = true;
						System.out.println("CRASH !!!!!! il peer " + key + " non risponde da " + difference);
						
						//qui il peer lo dichiaramo morto per cui gli inviamo il messaggio di morte per essere sicuri che 
						//forse lo abbiamo dichiarato morto per sbaglio
						
						currentview.get(key).ref.tell(PoisonPill.getInstance(), getSelf());
						//currentview.get(key).ref.tell(new isDead(), getSelf());
						
						tempView.remove(key);
					}
				}
				else{
					//qui se il timer è pari a 0 allora lo setto al corrente timer
					currentview.get(key).timer = current;
				}
				
			}
			
			if(crash){
				if(onChange){
					newIDview = newIDview+1;
				}else{
					newIDview = IDview+1;
				}
				
				if(newView != null){
					//faccio il controllo cosi da non duplicare icahnge view
					
					boolean firstcheck = newView.keySet().containsAll(tempView.keySet());
					boolean secondcheck = tempView.keySet().containsAll(newView.keySet());
					
					if(firstcheck && secondcheck){
						//sono uguali e quindi non replico il messaggio di change
						sendnewview = false;
						System.out.println("stavo per mandare una nuova change view duplicata ma non la invio");
					}
					
				}
				
				if(sendnewview){
					
					newView = new HashMap<>();
					newView.putAll(tempView);

					for (int key : newView.keySet()){
						newView.get(key).lastMessage = null;
						newView.get(key).timer = 0;
					}


					System.out.println("CRASH individuato e la nuova view e' : " + printView(newView));

					for (int key : newView.keySet()){
						System.out.println("mando il messaggio di changeview a: " + key + " --- " + newView.get(key).ref.toString());
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
			System.out.println("<"+id+"> Deliver nuovo messaggio con id : " + m.messageID + " mandato da: " 
					+ m.senderID + " inviata nella view : " + m.IDview + " siamo nella view " + IDview);
			log(" "+id+" deliver multicast " + m.messageID + " from " + m.senderID + " within " + IDview + "\n");
		}
		
		private void multicast(Map<Integer, actorData> v, genericMessage m){
			int timeSleep = rand.nextInt(1000) + 1000 ;
			if(m instanceof normalMessage){
				System.out.println("<"+id+"> SEND MULTICAST " + ((normalMessage)m).messageID 
						+ " NELLA VIEW " + ((normalMessage)m).IDview);
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
				
				if(crashNormal && id == 1 && messageID == 10 && (m instanceof normalMessage)){
					
					System.out.println("sono il peer 1 e sto mandando il messaggio 10 quindi mi fermo e simulo il crash");
					context().stop(getSelf());
					isCrash = true;
					break;
				}
				
				if(crashCache && id == 1 && (m instanceof cacheMessage)){
					System.out.println("sono il peer 1 e sto il messaggio di cache ma fingo il crash");
					context().stop(getSelf());
					isCrash = true;
					break;
				}
				
				
				if(crashFlush && id == 1  && IDview == 1 && (m instanceof FLUSH)){
					
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
		
		if(config.hasPath("nodeapp.joinPeer")){
			addJoinPeer = config.getBoolean("nodeapp.joinPeer");
		}
		
		if(myId == 0){
			System.out.println("Starting coordinator system");
		}
		// Create the actor system
		final ActorSystem system = ActorSystem.create("mysystem", config);

		
		// Create a single node actor locally
		if(myId == 0){
			final ActorRef receiver = system.actorOf(
					Node.props(myId, null),
					"node" // actor name
			);
		}
		else{
			final ActorRef receiver = system.actorOf(
					Node.props(myId, remotePath),
					"node" // actor name
			);
		}
		
		
		if(myId == 0 && addJoinPeer){
			system.actorOf(Node.props(myId - 1, remotePath),
				"peer1"
			);
			system.actorOf(Node.props(myId - 2, remotePath),
				"peer2"
			);
			
		}else if(myId == 0){
			System.out.println("sono il coordinator ma non inserisco nessun nuovo peer");
		}
	}
}
