package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.Master;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.map.HashedMap;

import java.io.File;
import java.util.*;

import static java.lang.Integer.max;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		boolean result;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new HashMap<>();

		this.batchMessages = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++) {
			this.batchMessages.add(new ArrayList<>());
		}

		this.countFinishRead = 0;
		this.unassignedTasks = new ArrayList<>();
		this.allTasks = new HashMap<int[], Boolean>();
		this.idleWorker = new ArrayList<>();
		this.workerTaskMap = new HashMap<ActorRef<DependencyWorker.Message>, int[]>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final Map<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> dependencyWorkers; //ActorRef<DependencyWorker.Message>, LMP of depWorker

	private final List<List<Set<String>>> batchMessages;

	private int countFinishRead;
	private final List<int[]> unassignedTasks;
	//private List<int[]> allTasks;
	private Map<int[], Boolean> allTasks;
	private final List<ActorRef<DependencyWorker.Message>> idleWorker;
	private final Map<ActorRef<DependencyWorker.Message>, int[]> workerTaskMap;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		for (int i = 0; i < headerLines[message.getId()].length; i++){
			batchMessages.get(message.getId()).add(new HashSet<>());
		}
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.
		// message: table_id & batch (ArrayList) attrs + value, size at max 10,000

		// Store BatchMessage such that the data can be accessed by column later on
		List<String[]> tmpBatchMessage = message.getBatch();
		for (String[] record: tmpBatchMessage){
			for (int i = 0; i < headerLines[message.getId()].length; i++){
				batchMessages.get(message.getId()).get(i).add(record[i]);
			}
		}

		if (tmpBatchMessage.size() != 0)
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		else {
			this.countFinishRead++;
			if (countFinishRead == 7){
				// prepare candidates after finish reading from all tables
				getCandidates();
			}
        }

		return this;
	}

	private void getCandidates(){
		List<int[]> attrList = new ArrayList<>();
		for (int file = 0; file < this.inputFiles.length; file++){
			for (int header = 0; header < this.headerLines[file].length; header++){
				int[] fileHeaderPair = {file, header};
				attrList.add(fileHeaderPair);
			}
		}
		// Store all candidates in unassignedTasks list
		for(int dep = 0; dep < attrList.size(); dep++){
			for(int ref = 0; ref < attrList.size(); ref++){
				if (dep == ref)
					continue;

				int[] tmp =	new int[4];
				System.arraycopy(attrList.get(dep), 0, tmp, 0, 2);
				System.arraycopy(attrList.get(ref), 0, tmp, 2, 2);
				this.unassignedTasks.add(tmp);
			}
		}

		for (int[] task: this.unassignedTasks){
			this.allTasks.put(task, null);
		}

		while(true){
			if(this.idleWorker.isEmpty()){
				break;
			}
			assignTask(this.idleWorker.get(0));
			this.idleWorker.remove(0);
		}
	}

	private void assignTask(ActorRef<DependencyWorker.Message> dependencyWorker){
		if (!this.unassignedTasks.isEmpty()){
			// assign task to worker
			int[] task = this.unassignedTasks.get(0);
			this.unassignedTasks.remove(0);

			// implement lmp to send task (send two columns: depColumn and refColumn
			List<Set<String>> taskColumns = new ArrayList<>();
			Set<String> depColumn = batchMessages.get(task[0]).get(task[1]);
			Set<String> refColumn = batchMessages.get(task[2]).get(task[3]);
			taskColumns.add(depColumn);
			taskColumns.add(refColumn);

			//Implement LMP to send task
			LargeMessageProxy.LargeMessage taskMessage = new DependencyWorker.TaskMessage(this.getContext().getSelf(), taskColumns);
			// LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), ind);
			//

			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskMessage, this.dependencyWorkers.get(dependencyWorker)));

			//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, taskColumns));
			//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.getContext().getSelf(), taskColumns));

			// add mapping of worker and task
			this.workerTaskMap.put(dependencyWorker, task);
		}else{
			this.idleWorker.add(dependencyWorker);
		}
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.containsKey(dependencyWorker)) {
			this.dependencyWorkers.put(dependencyWorker, message.dependencyWorkerLargeMessageProxy);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)

			// Create one list of idle workers, one list of unassigned tasks, mapping of worker to task
			// Check if there are unassigned tasks for workers, if not idle worker
			assignTask(dependencyWorker);

			//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));
			// lmp: data to bytecode, send piece by piece
			// note max size
			// lmp.SendMessage, be careful that sg replicated to be serialized when going thru lmp

		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.

		boolean result = message.result;
		int[] workerTask = workerTaskMap.get(dependencyWorker);
		// record the inclusion dependency if result == true
		if (result){
			File dependentFile = this.inputFiles[workerTask[0]];
			File referencedFile = this.inputFiles[workerTask[2]];
			String[] dependentAttribute = {this.headerLines[workerTask[0]][workerTask[1]]};
			String[] referencedAttribute = {this.headerLines[workerTask[2]][workerTask[3]]};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttribute, referencedFile, referencedAttribute);
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(ind);

			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}

		// remove the task from the workerTaskMap, record result
		this.workerTaskMap.put(dependencyWorker, null);
		this.allTasks.put(workerTask, result);

		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		// assign new task to the worker
		assignTask(dependencyWorker);

		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!

		// dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if(!this.allTasks.containsValue(null)){
			this.end();
		}

		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		this.getContext().getLog().info("Shutting down Dependency Miner!");
		return Behaviors.stopped();
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.getContext().getLog().info("Worker terminated signal received!");
		this.dependencyWorkers.remove(dependencyWorker);

		// if the worker is working on a task, need to reschedule the task, check the mapping
		if(this.workerTaskMap.containsKey(dependencyWorker) && this.workerTaskMap.get(dependencyWorker) != null){
			this.unassignedTasks.add(this.workerTaskMap.get(dependencyWorker));
			this.workerTaskMap.remove(dependencyWorker);
		}

		return this;
	}
}
