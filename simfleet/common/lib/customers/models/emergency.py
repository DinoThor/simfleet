import json
from random import randint
from typing import List

from loguru import logger
from spade.message import Message
from spade.template import Template

from simfleet.common.geolocatedagent import GeoLocatedAgent
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
)

from simfleet.common.agents.customer import CustomerAgent
from simfleet.utils.abstractstrategies import StrategyBehaviour
from simfleet.utils.messageconstants import REQUEST_EMERGENCY_CALL, INFORM_EMERGENCY_DONE


class EmergencyAgent(CustomerAgent):
    def __init__(self, agentjid, password):
        GeoLocatedAgent.__init__(self, agentjid, password)

        self._fleetmanagers = None
        self._patrol_assigned = None
        self._requested = False
        self._attendance_duration = randint(1,5)

    @property
    def fleetmanagers(self) -> List[str]:
        return self._fleetmanagers

    @fleetmanagers.setter
    def fleetmanagers(self, value: List[str]):
        self._fleetmanagers = value

    @property
    def patrol_assigned(self) -> str:
        return self._patrol_assigned

    @patrol_assigned.setter
    def patrol_assigned(self, value: str):
        self._patrol_assigned = value

    @property
    def duration(self):
        return self._attendance_duration

    @duration.setter
    def duration(self, value: int):
        self._attendance_duration = value

    @property
    def requested(self):
        return self._requested

    @requested.setter
    def requested(self, value: bool):
        self._requested = value


    def run_strategy(self):
        """import json
        Runs the strategy for the customer agent.
        """
        if not self.running_strategy:
            template1 = Template()
            template1.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template1)
            self.running_strategy = True

class EmergencyStrategyBehaviour(StrategyBehaviour):
    async def on_start(self) -> None:
        await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def send_request(self):
        if self.agent.fleetmanagers is not None:
            for fm in self.agent.fleetmanagers.keys():
                msg = Message()
                msg.to = str(fm)
                msg.set_metadata("protocol", REQUEST_PROTOCOL)
                msg.set_metadata("performative", REQUEST_PERFORMATIVE)
                msg.body = json.dumps({
                    "request": REQUEST_EMERGENCY_CALL,
                    "position": self.agent.get("current_pos")
                })
                await self.send(msg)
            logger.info(
                "Agent[{}]: The agent requested a patrol to ({}).".format(
                    self.agent.name, self.agent.get("current_pos")
                )
            )
            self.agent.requested = True

        else:
            logger.warning(
                "Agent[{}]: The agent has no fleet managers.".format(
                    self.agent.name
                )
            )

    async def accepted_patrol(self, patrol_id):
        """
        Stores the assigned patrol

        Args:
            patrol_id (str): The JID of the patrol agent to accept.
        """
        self.agent.patrol_assigned = str(patrol_id)
        logger.info(
            "Agent[{}]: The emergency is assigned to patrol [{}]".format(
                self.agent.name, patrol_id
            )
        )

    async def notify_patrol(self):
        """
        Notifies assisting patrol that the emergency is over.
        """
        msg = Message()
        msg.to = str(self.agent.patrol_assigned)
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps({
            "status": INFORM_EMERGENCY_DONE
        })
        await self.send(msg)

