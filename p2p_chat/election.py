from __future__ import annotations

import threading
from collections.abc import Callable


ELECTION_TIMEOUT = 3.0      # How long to wait for an OK reply before declaring ourselves leader
COORDINATOR_TIMEOUT = 5.0   # After receiving OK, how long to wait for the COORDINATOR message before retrying


class BullyElection:
    # Implements the Bully algorithm: the process with the highest ID
    # among the reachable members becomes the leader/coordinator.

    def __init__(
        self,
        process_id: int,
        get_members: Callable[[], set[int]],
        broadcast_packet: Callable[[dict], None],
        on_leader_change: Callable[[int], None] | None = None,
        get_username: Callable[[int], str | None] | None = None
    ):
        self.process_id = int(process_id)
        self.get_members = get_members            # Callback returning the current set of known process IDs
        self.broadcast_packet = broadcast_packet   # Callback used to send ELECTION/OK/COORDINATOR packets to peers

        self.on_leader_change = on_leader_change

        self.get_username = get_username  # username instead of raw ID for nicer log output

        self.leader_id: int | None = None
        self.election_in_progress = False

        self._ok_received = threading.Event()
        self._coordinator_received = threading.Event()

        self._lock = threading.RLock()
        self._election_generation = 0  # Increments per election so stale, delayed callbacks can be ignored

    def _describe(self, process_id: int) -> str:
        # Helper for logging: shows the username if known, otherwise the raw process ID
        if self.get_username is not None:
            try:
                username = self.get_username(process_id)

                if username:
                    return username

            except Exception:
                pass

        return f"process {process_id}"

    def start_election(self) -> None:
        # Kicks off a new Bully election. If another election is already
        # running, this call is ignored (only one election at a time)

        with self._lock:
            if self.election_in_progress:
                return

            self.election_in_progress = True
            self.leader_id = None

            self._ok_received.clear()
            self._coordinator_received.clear()

            self._election_generation += 1
            generation = self._election_generation

            members = self.get_members()

            # Bully rule: only processes with a higher ID may become leader,
            # so we only need to wait for those
            higher_processes = sorted(
                member_id
                for member_id in members
                if member_id > self.process_id
            )

        print(
            f"[BULLY] {self._describe(self.process_id)} "
            f"starts an election."
        )

        if not higher_processes:
            # No one with a higher ID exists -> we win immediately
            self._become_leader(generation)
            return

        self.broadcast_packet({
            "type": "ELECTION",
            "sender_id": self.process_id,
            "target_ids": higher_processes
        })

        # Wait for OK/COORDINATOR responses in a background thread so
        # start_election() itself doesn't block the caller.
        waiting_thread = threading.Thread(
            target=self._wait_for_result,
            args=(generation,),
            daemon=True
        )
        waiting_thread.start()

    def handle_packet(self, packet: dict) -> None:
        # Dispatches an incoming Bully control packet to the right handler
        packet_type = packet.get("type")

        if packet_type == "ELECTION":
            self._handle_election(packet)

        elif packet_type == "OK":
            self._handle_ok(packet)

        elif packet_type == "COORDINATOR":
            self._handle_coordinator(packet)

    def _handle_election(self, packet: dict) -> None:
        # Received an ELECTION message from a lower-ID process:
        # reply OK (we are alive and have a higher ID) and start our own election.

        try:
            sender_id = int(packet["sender_id"])

            target_ids = {
                int(target_id)
                for target_id in packet.get("target_ids", [])
            }

        except (KeyError, TypeError, ValueError):
            print("[BULLY ERROR] Invalid ELECTION packet.")
            return

        if target_ids and self.process_id not in target_ids:
            # This ELECTION wasn't addressed to us (broadcast reaches everyone,
            # but only the listed higher-ID targets should react).
            return

        if sender_id >= self.process_id:
            # Only respond if the sender has a lower ID than us
            return

        print(
            f"[BULLY] {self._describe(self.process_id)} received "
            f"ELECTION from {self._describe(sender_id)}."
        )

        self.broadcast_packet({
            "type": "OK",
            "sender_id": self.process_id,
            "target_id": sender_id
        })

        with self._lock:
            election_running = self.election_in_progress

        if not election_running:
            # We're not already in an election -> start our own,
            # since we outrank the original sender and might become leader.
            self.start_election()

    def _handle_ok(self, packet: dict) -> None:
        # Received an OK reply, meaning a higher-ID process is alive
        # and will take over the election; we should stand down and wait.

        try:
            sender_id = int(packet["sender_id"])
            target_id = int(packet["target_id"])

        except (KeyError, TypeError, ValueError):
            print("[BULLY ERROR] Invalid OK packet.")
            return

        if target_id != self.process_id:
            return

        if sender_id <= self.process_id:
            return

        print(
            f"[BULLY] {self._describe(sender_id)} answered OK."
        )

        self._ok_received.set()

    def _handle_coordinator(self, packet: dict) -> None:
        # Received a COORDINATOR announcement: accept it as the new leader,
        # unless our own ID is actually higher (defensive check).

        try:
            leader_id = int(packet["leader_id"])

        except (KeyError, TypeError, ValueError):
            print("[BULLY ERROR] Invalid COORDINATOR packet.")
            return

        if leader_id < self.process_id:
            # Should not normally happen under correct Bully behavior,
            # but if it does, reject this leader and re-run our own election.
            print(
                f"[BULLY] {self._describe(leader_id)} cannot be "
                f"leader because {self._describe(self.process_id)} "
                f"has the higher ID."
            )

            with self._lock:
                self.election_in_progress = False

            self.start_election()
            return

        with self._lock:
            self.leader_id = leader_id
            self.election_in_progress = False
            self._coordinator_received.set()

        print(
            f"[BULLY] {self._describe(leader_id)} is the new "
            f"leader."
        )

        self._notify_leader_change(leader_id)

    def _wait_for_result(self, generation: int) -> None:
        # Runs in a background thread after starting an election:
        # waits first for any OK, then for the COORDINATOR announcement,
        # taking over as leader or retrying if things time out.

        ok_received = self._ok_received.wait(
            ELECTION_TIMEOUT
        )

        with self._lock:
            if generation != self._election_generation:
                # A newer election has since started; this thread's result is stale.
                return

            if not self.election_in_progress:
                return

        if not ok_received:
            # No higher process answered in time -> we become the leader
            self._become_leader(generation)
            return

        coordinator_received = self._coordinator_received.wait(
            COORDINATOR_TIMEOUT
        )

        with self._lock:
            if generation != self._election_generation:
                return

            if not self.election_in_progress:
                return

        if coordinator_received:
            return

        # Someone answered OK but never announced themselves as coordinator
        # (e.g. they crashed mid-election) -> restart the election.
        print(
            "[BULLY] No COORDINATOR message was received. "
            "Restarting election."
        )

        with self._lock:
            self.election_in_progress = False

        self.start_election()

    def _become_leader(self, generation: int) -> None:
        # Declares this process the leader and announces it to everyone
        with self._lock:
            if generation != self._election_generation:
                return

            self.leader_id = self.process_id
            self.election_in_progress = False
            self._coordinator_received.set()

        print(
            f"[BULLY] {self._describe(self.process_id)} "
            f"becomes leader."
        )

        self.broadcast_packet({
            "type": "COORDINATOR",
            "sender_id": self.process_id,
            "leader_id": self.process_id
        })

        self._notify_leader_change(self.process_id)

    def handle_leader_failure(self) -> None:
        # Called when the current leader is detected as unreachable (via heartbeat timeout);
        # clears the leader state and triggers a fresh election.
        with self._lock:
            failed_leader = self.leader_id
            self.leader_id = None
            self.election_in_progress = False

        if failed_leader is not None:
            print(
                f"[BULLY] {self._describe(failed_leader)} failed."
            )

        self.start_election()

    def set_leader(self, leader_id: int) -> None:
        # Directly adopts a leader learned from another source (a PEER_LIST message from the leader itself),
        #  without running a full election.
        with self._lock:
            if self.leader_id == leader_id:
                return

            self.leader_id = leader_id
            self.election_in_progress = False
            self._coordinator_received.set()

        print(
            f"[BULLY] Adopting {self._describe(leader_id)} as "
            f"leader (learned from peer list)."
        )

        self._notify_leader_change(leader_id)

    def get_leader(self) -> int | None:
        # Thread-safe read of the currently known leader ID
        with self._lock:
            return self.leader_id

    def _notify_leader_change(self, leader_id: int) -> None:
        # Invokes the external callback (if any) whenever the leader changes,
        # guarding against exceptions raised by that callback.
        if self.on_leader_change is None:
            return

        try:
            self.on_leader_change(leader_id)
        except Exception as error:
            print(
                f"[BULLY ERROR] on_leader_change callback failed: "
                f"{error}"
            )