import asyncio
import json
from loguru import logger
from asyncio import CancelledError

from simfleet.common.lib.customers.models.emergency import EmergencyStrategyBehaviour
from simfleet.communications.protocol import INFORM_PERFORMATIVE, ACCEPT_PERFORMATIVE
from simfleet.utils.status import (
    EMERGENCY_PATROL_ARRIVED,
    EMERGENCY_DONE,
    EMERGENCY_WAITING,
    EMERGENCY_PATROL_MOVING_TO,
    EMERGENCY_PATROL_ASSIGNED,
)

from simfleet.utils.messageconstants import INFORM_MOVING_TO, INFORM_ARRIVED

class EmergencyBehaviour(EmergencyStrategyBehaviour):
    """
    An emergency strategy behaviour that accepts the first patrol proposal

    Inherits from:
        EmergencyStrategyBehaviour: A base class for customer behaviors in the emergency alert definitions.

    Methods:
        run(): The main coroutine responsible for the strategy's execution. It listens for messages
               and reacts based on the agent's current status and the message's performative.
    """

    async def on_start(self) -> None:
        await super().on_start()
        logger.debug("Emergency [{}] started EmergencyBehaviour".format(self.agent.name))

    async def run(self):
        """
                The core coroutine that implements the customer strategy.
                It handles message receiving, proposal acceptance, refusal, and status updates.
                The customer will accept the first valid transport proposal and update its status
                based on the incoming messages from transport agents.
        """
        if self.agent.status == EMERGENCY_DONE:
            return

        if self.agent.status is None:
            self.agent.status = EMERGENCY_WAITING
            return

        if self.agent.fleetmanagers is None:
            fleetmanager_list = await self.agent.get_list_agent_position(
                self.agent.fleet_type, self.agent.fleetmanagers
            )
            self.agent.fleetmanagers = fleetmanager_list
            return

        if self.agent.status == EMERGENCY_WAITING:
            if self.agent.requested:
                return

            self.agent.events_store.emit(
                event_type="emergency_request",
                details={}
            )
            await self.send_request()

        try:
            msg = await self.receive(timeout=60)

            if msg:
                performative = msg.get_metadata("performative")
                patrol_id = msg.sender

                if performative == ACCEPT_PERFORMATIVE:
                    if self.agent.status == EMERGENCY_WAITING:
                        logger.debug(
                            "Agent[{}]: The patrol ({}) is moving to emergency in ({})".format(
                                self.agent.name, patrol_id, self.agent.get("current_pos")
                            )
                        )

                        self.agent.events_store.emit(
                            event_type="wait_for_patrol",
                            details={}
                        )

                        await self.accepted_patrol(str(patrol_id))
                        self.agent.status = EMERGENCY_PATROL_ASSIGNED

                elif performative == INFORM_PERFORMATIVE:
                    try:
                        content = json.loads(msg.body)
                    except TypeError:
                        content = None

                    if "status" in content:
                        status = content["status"]

                        if status == INFORM_MOVING_TO and self.agent.status == EMERGENCY_PATROL_ASSIGNED:
                            logger.info(
                                "Agent[{}] waiting for patrol ({}).".format(
                                    self.agent.name, self.agent.patrol_assigned
                                )
                            )
                            self.agent.status = EMERGENCY_PATROL_MOVING_TO

                        elif status == INFORM_ARRIVED and self.agent.status == EMERGENCY_PATROL_MOVING_TO:
                            logger.info("Agent[{}]: patrol ({}) is attending emergency.".format(
                                    self.agent.name, self.agent.patrol_assigned
                                )
                            )
                            self.agent.status = EMERGENCY_PATROL_ARRIVED

                            self.agent.events_store.emit(
                                event_type="patrol_arrived",
                                details={}
                            )

                            await asyncio.sleep(self.agent.duration)
                            self.agent.status = EMERGENCY_DONE

                            self.agent.events_store.emit(
                                event_type="emergency_done",
                                details={}
                            )
                            await self.notify_patrol()


        except CancelledError:
            logger.debug("Cancelling async tasks...")

        except Exception as e:
            logger.error(
                "EXCEPTION in EmergencyBehaviour of agent [{}]: {}".format(
                    self.agent.name, e
                )
            )
