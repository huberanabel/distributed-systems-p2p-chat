from __future__ import annotations

import threading
from collections.abc import Callable


ELECTION_TIMEOUT = 3.0
COORDINATOR_TIMEOUT = 5.0


class BullyElection:
    """
    Implementation of the Bully leader-election algorithm.

    Rules:
    - Every peer has a unique integer process ID.
    - The active peer with the highest process ID becomes leader.
    - ELECTION is sent to peers with higher IDs.
    - Higher peers answer with OK.
    - The winner sends a COORDINATOR message to all peers.
    """

    def __init__(
        self,
        process_id: int,
        get_members: Callable[[], set[int]],
        broadcast_packet: Callable[[dict], None]
    ):
        self.process_id = int(process_id)

        # Returns the IDs of all currently known peers.
        self.get_members = get_members

        # Sends one control packet to all connected peers.
        self.broadcast_packet = broadcast_packet

        self.leader_id: int | None = None
        self.election_in_progress = False

        self._ok_received = threading.Event()
        self._coordinator_received = threading.Event()

        self._lock = threading.RLock()
        self._election_generation = 0

    def start_election(self) -> None:
        """
        Starts a new Bully election.

        The peer contacts all known peers with a higher process ID.
        If no higher peer exists, this peer becomes the leader.
        """

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

            higher_processes = sorted(
                member_id
                for member_id in members
                if member_id > self.process_id
            )

        print(
            f"[BULLY] Process {self.process_id} starts an election."
        )

        if not higher_processes:
            self._become_leader(generation)
            return

        self.broadcast_packet({
            "type": "ELECTION",
            "sender_id": self.process_id,
            "target_ids": higher_processes
        })

        waiting_thread = threading.Thread(
            target=self._wait_for_result,
            args=(generation,),
            daemon=True
        )
        waiting_thread.start()

    def handle_packet(self, packet: dict) -> None:
        """
        Handles one incoming election-control packet.
        """

        packet_type = packet.get("type")

        if packet_type == "ELECTION":
            self._handle_election(packet)

        elif packet_type == "OK":
            self._handle_ok(packet)

        elif packet_type == "COORDINATOR":
            self._handle_coordinator(packet)

    def _handle_election(self, packet: dict) -> None:
        """
        Handles an ELECTION message from a lower process.

        A higher process answers with OK and starts its own election.
        """

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
            return

        if sender_id >= self.process_id:
            return

        print(
            f"[BULLY] Process {self.process_id} received "
            f"ELECTION from process {sender_id}."
        )

        self.broadcast_packet({
            "type": "OK",
            "sender_id": self.process_id,
            "target_id": sender_id
        })

        with self._lock:
            election_running = self.election_in_progress

        if not election_running:
            self.start_election()

    def _handle_ok(self, packet: dict) -> None:
        """
        Handles an OK response from a higher process.
        """

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
            f"[BULLY] Higher process {sender_id} answered OK."
        )

        self._ok_received.set()

    def _handle_coordinator(self, packet: dict) -> None:
        """
        Handles the announcement of the new leader.
        """

        try:
            leader_id = int(packet["leader_id"])

        except (KeyError, TypeError, ValueError):
            print("[BULLY ERROR] Invalid COORDINATOR packet.")
            return

        if leader_id < self.process_id:
            print(
                f"[BULLY] Process {leader_id} cannot be leader "
                f"because this process has the higher ID "
                f"{self.process_id}."
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
            f"[BULLY] Process {leader_id} is the new leader."
        )

    def _wait_for_result(self, generation: int) -> None:
        """
        Waits for answers from higher processes.

        No OK received:
            this process becomes leader.

        OK received:
            wait for a COORDINATOR message.
        """

        ok_received = self._ok_received.wait(
            ELECTION_TIMEOUT
        )

        with self._lock:
            if generation != self._election_generation:
                return

            if not self.election_in_progress:
                return

        if not ok_received:
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

        print(
            "[BULLY] No COORDINATOR message was received. "
            "Restarting election."
        )

        with self._lock:
            self.election_in_progress = False

        self.start_election()

    def _become_leader(self, generation: int) -> None:
        """
        Marks this process as leader and announces the result.
        """

        with self._lock:
            if generation != self._election_generation:
                return

            self.leader_id = self.process_id
            self.election_in_progress = False
            self._coordinator_received.set()

        print(
            f"[BULLY] Process {self.process_id} becomes leader."
        )

        self.broadcast_packet({
            "type": "COORDINATOR",
            "sender_id": self.process_id,
            "leader_id": self.process_id
        })

    def handle_leader_failure(self) -> None:
        """
        Starts a new election after the current leader disconnects.

        peer.py calls this method when it detects that
        the leader has failed.
        """

        with self._lock:
            failed_leader = self.leader_id
            self.leader_id = None
            self.election_in_progress = False

        if failed_leader is not None:
            print(
                f"[BULLY] Leader {failed_leader} failed."
            )

        self.start_election()

    def get_leader(self) -> int | None:
        """
        Returns the currently known leader ID.
        """

        with self._lock:
            return self.leader_id