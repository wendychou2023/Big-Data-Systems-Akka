package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		//ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		ActorRef<DependencyMiner.Message> replyTo;
		List<Set<String>> task;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");
		// I should probably know how to solve this task, but for now I just pretend some work...

		boolean ind = true;
		String[] depColumn = Arrays.copyOf(message.getTask().get(0).toArray(), message.getTask().get(0).size(), String[].class);
		String[] refColumn = Arrays.copyOf(message.getTask().get(1).toArray(), message.getTask().get(1).size(), String[].class);

		int depColumnSize = depColumn.length;
		int refColumnSize = refColumn.length;

		if (depColumnSize > refColumnSize){
			ind = false;
		}else{
			// Check Inclusion Dependency: if depColumn c refColumn
			HashSet<String> hashSet = new HashSet<>();

			// hashSet stores all the values of refColumn
			for (int i = 0; i < refColumnSize; i++) {
				if (!hashSet.contains(refColumn[i]))
					hashSet.add(refColumn[i]);
			}

			// loop to check if all elements of depColumn also lies in refColumn
			for (int i = 0; i < depColumnSize; i++) {
				if (!hashSet.contains(depColumn[i])) {
					ind = false;
					break;
				}
			}
		}

		//LargeMessageProxy.LargeMessage completionMessage = (LargeMessageProxy.LargeMessage) new DependencyMiner.CompletionMessage(this.getContext().getSelf(), ind);
		//this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));
		// Use regular message sending instead of LMP for CompletionMsg
		message.getReplyTo().tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), ind));

		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		this.getContext().getLog().info("Shutting down Dependency Worker!");
		return Behaviors.stopped();
	}
}
