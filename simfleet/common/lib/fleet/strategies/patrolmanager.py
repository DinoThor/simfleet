import asyncio
import json
from random import choices
from string import ascii_lowercase, digits

from loguru import logger
from spade.behaviour import OneShotBehaviour
from spade.message import Message
from spade.template import Template

from simfleet.common.agents.fleetmanager import FleetManagerStrategyBehaviour
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    INFORM_PERFORMATIVE
)
from simfleet.utils.messageconstants import (
    REQUEST_CALCULATE_POSITION,
    ACCEPT_PATROL,
    INFORM_BACK_TO_ROUTE
)

class PositionResponseQueue(OneShotBehaviour):
    """
    Behaviour designed to send position calculation petitions and await the
    responses. It's made as a separated behaviour from PatrolCoordinatorBehaviour
    in order to use a different template that filters via thread_id and avoid
    conflicts in the reception process
    """
    def __init__(self, patrols, position, **kwargs):
        super().__init__(**kwargs)
        self.patrols = patrols
        self.position = position

    async def run(self):
        requests = []
        for p in self.patrols:
            request_msg = Message()
            request_msg.to = p
            request_msg.thread = self.template.thread
            request_msg.set_metadata("protocol", REQUEST_PROTOCOL)
            request_msg.set_metadata("performative", REQUEST_PERFORMATIVE)
            request_msg.body = json.dumps({
                "request": REQUEST_CALCULATE_POSITION,
                "position": self.position
            })
            requests.append(request_msg)

        send_requests = [self.send(msg) for msg in requests]
        requests = await asyncio.gather(*send_requests)
        gather_results = [self.receive(timeout=10) for _ in range(len(requests))]
        results = await asyncio.gather(*gather_results)
        self.kill(exit_code=results)


class PatrolCoordinatorBehaviour(FleetManagerStrategyBehaviour):
    alphabet = ascii_lowercase + digits

    def thread_id_generator(self):
        return ''.join(choices(self.alphabet, k=8))

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

        try:
            msg = await self.receive(timeout=60)
            if msg:
                logger.debug("Manager received message: {}".format(msg))

                protocol = msg.get_metadata("protocol")
                performative = msg.get_metadata("performative")
                if protocol == REQUEST_PROTOCOL:
                    if performative == REQUEST_PERFORMATIVE:
                        emergency_position = json.loads(msg.body)["position"]
                        patrols = [
                            v["jid"]
                            for v in self.get_transport_agents().values()
                            if v["jid"] not in (self.agent.get("busy_patrol_list") or [])
                        ]
                        if len(patrols) < 1:
                            pending = self.agent.get("pending_emergencies") or []
                            pending.append((msg.sender, emergency_position))
                            self.agent.set("pending_emergencies", pending)
                            return

                        thread_id = self.thread_id_generator()

                        template = Template()
                        template.thread = thread_id

                        beh = PositionResponseQueue(patrols, emergency_position)
                        self.agent.add_behaviour(beh, template)
                        await beh.join()

                        closest_patrol = min(
                            beh.exit_code,
                            key=lambda p: json.loads(p.body)["result"]
                        )

                        if closest_patrol:
                            fwd_msg = Message()
                            fwd_msg.sender = msg.sender
                            fwd_msg.to = closest_patrol.sender
                            fwd_msg.set_metadata("protocol", REQUEST_PROTOCOL)
                            fwd_msg.set_metadata("performative", REQUEST_PERFORMATIVE)
                            fwd_msg.body = msg.body
                            await self.send(fwd_msg)

                    elif performative == ACCEPT_PERFORMATIVE:
                        try:
                            content = json.loads(msg.body)
                        except TypeError:
                            return

                        if content.get("status") == ACCEPT_PATROL:
                            patrol_id = str(msg.sender)
                            busy_list = self.agent.get("busy_patrol_list") or []
                            busy_list.append(patrol_id)
                            self.agent.set("busy_patrol_list", busy_list)

                    elif performative == INFORM_PERFORMATIVE:
                        try:
                            content = json.loads(msg.body)
                        except TypeError:
                            return

                        if content.get("status") == INFORM_BACK_TO_ROUTE:
                            pending = self.agent.get("pending_emergencies") or []
                            if len(pending) > 0:
                                emergency_id, emergency_position = pending.pop()
                                pending_msg = Message()
                                pending_msg.sender = emergency_id
                                pending_msg.to = msg.sender
                                pending_msg.set_metadata("protocol", REQUEST_PROTOCOL)
                                pending_msg.set_metadata("performative", REQUEST_PERFORMATIVE)
                                pending_msg.body = msg.body
                                await self.send(pending_msg)
                            else:
                                patrol_id = str(msg.sender)
                                busy = self.agent.get("busy_patrol_list") or []
                                busy.remove(patrol_id)
                                self.agent.set("busy_patrol_list", busy)

        except asyncio.CancelledError:
            logger.debug("Cancelling async tasks...")

        except Exception as e:
            logger.error(
                "EXCEPTION in PatrolCoordinatorBehaviour of agent [{}]: {}".format(
                    self.agent.name, e
                )
            )
