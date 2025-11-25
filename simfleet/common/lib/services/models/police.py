import asyncio
import json
import sys
from typing import Union

from loguru import logger
from asyncio import CancelledError
from spade.behaviour import CyclicBehaviour
from spade.message import Message
from spade.template import Template
from spade.behaviour import State

from simfleet.common.agents.transport import TransportAgent
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    INFORM_PERFORMATIVE,
    REGISTER_PROTOCOL,
    REQUEST_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)
from simfleet.utils.helpers import PathRequestException, AlreadyInDestination, distance_in_meters
from simfleet.utils.messageconstants import (
    REQUEST_EMERGENCY_CALL,
    REQUEST_CALCULATE_POSITION,
    INFORM_BACK_TO_ROUTE,
    INFORM_ARRIVED,
    INFORM_MOVING_TO,
    ACCEPT_PATROL
)
from simfleet.common.mixins.movable import MovingBehaviour


class PolicePatrolAgent(TransportAgent):
    """
    Represents a police agent in a patrol. Manages the route, response to an emergency,
    and interactions with criminals or emergencies requests.
    """
    def __init__(self, agentjid, password, **kwargs):
        super().__init__(agentjid, password)

        self.fleetmanager_id = kwargs.get('fleet', None)

        # Police station base
        self._base = None
        self._going_to_base = False

        # Route line attributes
        self._route_points_list = []
        self._route_type = None
        self._next_point = None

        # Emergency inbox
        self._emergency_id = None
        self._emergency_coors = None

        # Attending inbox
        self._attend_emergency_queue = asyncio.Queue()

        # Emergency call event
        self.set("patrol_requested", None)
        self.patrol_requested_event = asyncio.Event()

        # Transport in stop event
        self.set("arrived_to_point", None)  # new
        self.patrol_arrived_to_point_event = asyncio.Event()

        def patrol_arrived_to_point_callback(old, new):
            if not self.patrol_arrived_to_point_event.is_set() and new is True:
                self.patrol_arrived_to_point_event.set()

        self.patrol_arrived_to_point_callback = patrol_arrived_to_point_callback

    @property
    def base(self):
        return self._base

    @base.setter
    def base(self, value):
        self._base = value

    @property
    def going_to_base(self):
        return self._going_to_base

    @going_to_base.setter
    def going_to_base(self, value):
        self._going_to_base = value

    @property
    def route_points_list(self):
        return self._route_points_list

    @route_points_list.setter
    def route_points_list(self, value):
        self._route_points_list = value

    @property
    def route_type(self):
        return self._route_type

    @route_type.setter
    def route_type(self, value):
        self._route_type = value

    @property
    def emergency_coors(self):
        return self._emergency_coors

    @emergency_coors.setter
    def emergency_coors(self, value):
        self._emergency_coors = value

    @property
    def emergency_id(self):
        return self._emergency_id

    @emergency_id.setter
    def emergency_id(self, value):
        self._emergency_id = value

    @property
    def attend_emergency_queue(self):
        return self._attend_emergency_queue

    @property
    def next_point(self):
        return self._next_point

    @next_point.setter
    def next_point(self, value):
        self._next_point = value

    async def setup(self):
        """
        Sets up the transport agent with the registration behavior.
        """
        try:
            template = Template()
            template.set_metadata("protocol", REGISTER_PROTOCOL)
            register_behaviour = RegistrationBehaviour()
            self.add_behaviour(register_behaviour, template)
            while not self.has_behaviour(register_behaviour):
                logger.warning(
                    "Transport {} could not create RegisterBehaviour. Retrying...".format(
                        self.agent_id
                    )
                )
                self.add_behaviour(register_behaviour, template)
        except Exception as e:
            logger.error(
                "EXCEPTION creating RegisterBehaviour in Transport {}: {}".format(
                    self.agent_id, e
                )
            )

        try:
            template = Template()
            template.set_metadata("protocol", REQUEST_PROTOCOL)
            emergency_behaviour = EmergencyNotifierBehaviour()
            self.add_behaviour(emergency_behaviour, template)
            while not self.has_behaviour(emergency_behaviour):
                logger.warning(
                    "Transport {} could not create EmergencyNotifierBehaviour. Retrying...".format(
                        self.agent_id
                    )
                )
                self.add_behaviour(emergency_behaviour, template)
            self.ready = True
        except Exception as e:
            logger.error(
                "EXCEPTION creating EmergencyNotifierBehaviour in Transport {}: {}".format(
                    self.agent_id, e
                )
            )

    def run_strategy(self):
        """
        Sets the strategy for the transport agent.

        Args: strategy_class (``PoliceStrategyBehaviour``): The class to be used. Must inherit from
        ``PoliceStrategyBehaviour``
        """
        if not self.running_strategy:
            template1 = Template()
            template1.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template1)
            self.running_strategy = True

    def set_line(self, line):
        """
        Does not apply in PolicePatrolAgent
        """
        pass

    def set_line_type(self, line_type):
        self.route_type = line_type

    def set_stop_list(self, stop_list):
        """
        Sets the list of stops for the bus line.

        Args:
            stop_list (list): List of stops in format (lat, lng).
        """
        self._route_points_list = stop_list


    async def set_position(self, coords=None):
        """
        Sets the position of the patrol.

        Args:
            coords (tuple): a tuple of coordinates (longitude and latitude)
        """

        await super().set_position(coords)
        self.set("current_pos", coords)

        if coords == self.dest:
            logger.info(
                "Patrol {} has arrived to destination".format(self.agent_id)
            )
            await self.arrived_to_point()

    def setup_current_point(self):
        """
        Sets the current stop based on the transport's position.
        """
        current_pos = self.get("current_pos")

        try:
            index = self.route_points_list.index(current_pos)
            self.latest_point = self.route_points_list[index]
            return True
        except ValueError:
            return False

    async def arrived_to_point(self):
        """
        Marks the current stop as arrived and triggers the event.
        """
        self.set("arrived_to_point", True)


class RegistrationBehaviour(CyclicBehaviour):
    """
    Manages the registration process for the bus agent in the fleet.

    Methods:
        on_start(): Initializes the registration behavior.
        send_registration(): Sends a registration proposal to the fleet manager.
        run(): Executes the behavior, handling registration acceptance or rejection.
    """
    async def on_start(self):
        logger.debug("Strategy {} started in transport".format(type(self).__name__))

    async def send_registration(self):
        """
            Sends a registration proposal message to the fleet manager.
        """
        logger.debug(
            "Transport {} sent proposal to register to manager {}".format(
                self.agent.name, self.agent.fleetmanager_id
            )
        )
        content = {
            "name": self.agent.name,
            "jid" : str(self.agent.jid),
            "fleet_type": self.agent.fleet_type,
        }
        msg = Message()
        msg.to = str(self.agent.fleetmanager_id)
        msg.set_metadata("protocol", REGISTER_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)
        await self.send(msg)

    async def run(self):
        try:
            if not self.agent.registration:
                await self.send_registration()
            msg = await self.receive(timeout=10)
            if msg:
                performative = msg.get_metadata("performative")
                if performative == ACCEPT_PERFORMATIVE:
                    content = json.loads(msg.body)
                    self.agent.set_registration(True, content)
                    logger.info(
                        "[{}] Registration in the fleet manager accepted: {}.".format(
                            self.agent.name, self.agent.fleetmanager_id
                        )
                    )
                    self.kill(exit_code="Fleet Registration Accepted")
                elif performative == REFUSE_PERFORMATIVE:
                    logger.warning(
                        "Registration in the fleet manager was rejected (check fleet type)."
                    )
                    self.kill(exit_code="Fleet Registration Rejected")
        except CancelledError:
            logger.debug("Cancelling async tasks...")
        except Exception as e:
            logger.error(
                "EXCEPTION in RegisterBehaviour of Transport {}: {}".format(
                    self.agent.name, e
                )
            )


class PoliceStrategyBehaviour(State):
    """
    Class to define a transport strategy for the bus agent. Inherit from this class to implement custom strategies.

    Helper functions:
        - send_get_stops
        - get_subsequent_stop
        - move_to_next_stop
    """

    async def run(self):
        raise NotImplementedError

    async def on_start(self):
        logger.debug(
            "Strategy {} started in transport {}".format(
                type(self).__name__, self.agent.name
            )
        )

    def get_next_point(self) -> Union[tuple[str, str], None]:
        """
            Gets the next point in the route based on the current location.
        """
        try:
            index_current = self.agent.route_points_list.index(self.agent.get("current_pos"))
        except ValueError:
            index_current = None
        if index_current is None:
            logger.critical("Transport {} current pos ({}) is not in its stop_list {}".format(
                self.agent.name, self.agent.get("current_pos"),self.agent.route_points_list)
            )
            sys.exit()
        next_point = None
        if index_current + 1 < len(self.agent.route_points_list):
            next_point = self.agent.route_points_list[index_current + 1]
        return next_point

    async def move_to_point(self, next_point) -> Union[MovingBehaviour, None]:
        """
            Moves the patrol to the next point.
        """
        logger.info("Patrol {} in route to {}".format(self.agent.name, next_point))
        self.agent.set("next_pos", next_point)
        try:
            behav: MovingBehaviour = await self.agent.move_to(next_point)
            return behav
        except AlreadyInDestination:
            self.agent.dest = next_point
            await self.agent.arrived_to_point()
            return None
        except PathRequestException as e:
            logger.error(
                "Raising PathRequestException in pick_up_customer for {}".format(
                    self.agent.name
                )
            )
            raise e

    async def clear_emergency(self):
        self.agent.emergency_id = None
        self.agent.emergency_coors = None

    async def notify_moving_to(self):
        msg = Message()
        msg.to = self.agent.emergency_id
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps({
            "status": INFORM_MOVING_TO
        })
        await self.send(msg)

    async def notify_arrived(self):
        msg = Message()
        msg.to = self.agent.emergency_id
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps({
            "status": INFORM_ARRIVED
        })
        await self.send(msg)

    async def notify_back_to_route(self):
        msg = Message()
        msg.to = self.agent.fleetmanager_id
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps({
            "status": INFORM_BACK_TO_ROUTE
        })
        await self.send(msg)

    async def attend_queue_get(self):
        """
        Returns the next item in the queue of messages from an EmergencyAgent
        """
        return await self.agent.attend_emergency_queue.get()

class EmergencyNotifierBehaviour(CyclicBehaviour):
    """
    Cyclic Behaviour used as middleware to receive
    emergency request, interrupt the route flow and
    attend the petition
    """
    async def run(self):
        msg = await self.receive(timeout=60)
        if msg:
            if msg.sender == self.agent.emergency_id:
                self.agent.attend_emergency_queue.put_nowait(msg)
                return

            protocol = msg.get_metadata("protocol")
            performative = msg.get_metadata("performative")
            if protocol == REQUEST_PROTOCOL:
                if performative == REQUEST_PERFORMATIVE:
                    body = json.loads(msg.body)
                    if body:
                        request = body["request"]
                        if request == REQUEST_CALCULATE_POSITION and msg.sender == self.agent.fleetmanager_id:
                            emergency_position = body["position"]
                            await self.position_response(msg.make_reply(), emergency_position)
                        elif request == REQUEST_EMERGENCY_CALL:
                            self.agent.emergency_coors = body["position"]
                            self.agent.emergency_id = str(msg.sender)

                            self.agent.patrol_requested_event.set()
                            await self.notify_accept()

                elif performative == INFORM_PERFORMATIVE:
                    pass

    async def position_response(self, reply, emergency_position: tuple[float, float]):
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", INFORM_PERFORMATIVE)
        reply.body = json.dumps({
            "request": REQUEST_CALCULATE_POSITION,
            "result": distance_in_meters(
                self.agent.get("current_pos"), emergency_position
            )
        })
        await self.send(reply)

    async def notify_accept(self):
        manager_msg = Message()
        manager_msg.to = self.agent.fleetmanager_id
        manager_msg.set_metadata("protocol", REQUEST_PROTOCOL)
        manager_msg.set_metadata("performative", ACCEPT_PERFORMATIVE)
        manager_msg.body = json.dumps({
            "status": ACCEPT_PATROL
        })
        await self.send(manager_msg)

        emergency_msg = Message()
        emergency_msg.to = self.agent.emergency_id
        emergency_msg.set_metadata("protocol", REQUEST_PROTOCOL)
        emergency_msg.set_metadata("performative", ACCEPT_PERFORMATIVE)
        await self.send(emergency_msg)
