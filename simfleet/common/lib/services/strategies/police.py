import asyncio
import json
from enum import Enum

from loguru import logger

from simfleet.common.lib.services.models.police import PoliceStrategyBehaviour
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour
from simfleet.communications.protocol import REQUEST_PROTOCOL, INFORM_PERFORMATIVE
from simfleet.utils.messageconstants import INFORM_EMERGENCY_DONE


################################################################
#                                                              #
#                      Police Strategy                         #
#                                                              #
################################################################

class PoliceState(Enum):
    START_FROM_BASE = 'START_FROM_BASE'
    NEXT_POINT = 'NEXT_POINT'
    MOVING_TO = 'MOVING_TO'
    ARRIVED_POINT = 'ARRIVED_POINT'
    EMERGENCY_CALL = 'EMERGENCY_CALL'
    ATTEND_EMGY = 'ATTEND_EMGY'
    END_ROUTE = 'END_ROUTE'
    BACK_INTO_BASE = 'BACK_INTO_BASE'

class PoliceStart(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.START_FROM_BASE.value
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.START_FROM_BASE.value))

    async def run(self):
        self.agent.base = self.agent.get("current_pos")
        logger.debug("Patrol {} in base {})".format(self.agent.jid, self.agent.base))
        self.set_next_state(PoliceState.NEXT_POINT.value)


class PoliceNextPointState(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.NEXT_POINT.value
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.NEXT_POINT.value))

    async def run(self):
        self.agent.next_point = self.get_next_point()
        if self.agent.next_point is None:
            if self.agent.route_type == "circular":
                self.agent.next_point = self.agent.route_points_list[0]
            else:
                logger.info("Patrol {} finished route. Back to base".format(self.agent.name))
                self.agent.going_to_base = True
                self.agent.next_point = self.agent.base

        if self.agent.patrol_requested_event.is_set():
            self.set_next_state(PoliceState.EMERGENCY_CALL.value)
        else:
            behav = await self.move_to_point(self.agent.next_point)
            self.agent.set("movement_behav", behav)
            self.set_next_state(PoliceState.MOVING_TO.value)
        return


class PoliceMovingToNextPointState(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.MOVING_TO.value
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.MOVING_TO.value))

        if self.agent.emergency_id and self.agent.emergency_coors:
            await self.notify_moving_to()

    async def run(self):
        if self.agent.is_in_destination():
            return self.set_next_state(PoliceState.ARRIVED_POINT.value)

        self.agent.patrol_arrived_to_point_event.clear()
        self.agent.watch_value("arrived_to_point", self.agent.patrol_arrived_to_point_callback)

        # async def wait_watcher(name, event):
        #     await event.wait()
        #     return name

        # watchers = [
        #     asyncio.create_task(
        #         wait_watcher("arrived", self.agent.patrol_arrived_to_point_event)
        #     ),  # Arrived at point
        #     asyncio.create_task(
        #         wait_watcher("emergency", self.agent.patrol_requested_event)
        #     )  # Emergency call
        # ]

        while not (
            self.agent.patrol_arrived_to_point_event.is_set()
            or self.agent.patrol_requested_event.is_set()
        ):
            self.agent.events_store.emit(
                event_type="patrol_moving",
                details={
                    "location": self.agent.get("current_pos")
                }
            )
            await asyncio.sleep(0.5)

        # done, pending = await asyncio.wait(watchers, return_when=asyncio.FIRST_COMPLETED)
        # first = done.pop()

        if self.agent.patrol_arrived_to_point_event.is_set():
            return self.set_next_state(PoliceState.ARRIVED_POINT.value)
        else:
            return self.set_next_state(PoliceState.EMERGENCY_CALL.value)


class PoliceArrivedAtPointState(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.ARRIVED_POINT.value
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.ARRIVED_POINT.value))

    async def run(self):
        if self.agent.going_to_base and self.agent.get("current_pos") == self.agent.base:
            return self.set_next_state(PoliceState.BACK_INTO_BASE.value)
        if self.agent.emergency_id and self.agent.emergency_coors:
            return self.set_next_state(PoliceState.ATTEND_EMGY.value)
        if self.agent.patrol_requested_event.is_set():
            return self.set_next_state(PoliceState.EMERGENCY_CALL.value)

        return self.set_next_state(PoliceState.NEXT_POINT.value)


class PoliceEmergencyCallState(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.EMERGENCY_CALL.value
        self.agent.patrol_requested_event.clear()
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.EMERGENCY_CALL.value))

    async def run(self):
        behav = await self.move_to_point(self.agent.emergency_coors)
        self.agent.set("movement_behav", behav)
        self.set_next_state(PoliceState.MOVING_TO.value)
        return


class PoliceAttendEmergencyState(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.ATTEND_EMGY.value
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.ATTEND_EMGY.value))

    async def run(self):
        await self.notify_arrived()
        msg = await self.attend_queue_get()
        if msg:
            protocol = msg.get_metadata("protocol")
            performative = msg.get_metadata("performative")
            if protocol != REQUEST_PROTOCOL and performative != INFORM_PERFORMATIVE:
                self.agent.attend_emergency_queue.put_nowait(msg)
                return
            content = json.loads(msg.body)
            if "status" not in content and content["status"] != INFORM_EMERGENCY_DONE:
                self.agent.attend_emergency_queue.put_nowait(msg)
                return

            await self.clear_emergency()
            behav = await self.move_to_point(self.agent.next_point) # Back to route
            self.agent.set("movement_behav", behav)
            await self.notify_back_to_route()
            self.set_next_state(PoliceState.MOVING_TO.value)
            return

        self.set_next_state(PoliceState.ATTEND_EMGY.value)
        return

class PoliceEndRoute(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.END_ROUTE.value
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.END_ROUTE.value))

    async def run(self):
        await self.move_to_point(self.agent.base)
        self.set_next_state(PoliceState.MOVING_TO.value)
        return


class PoliceBackIntoBase(PoliceStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = PoliceState.BACK_INTO_BASE.value
        logger.debug("Patrol {} in {} State".format(self.agent.name, PoliceState.BACK_INTO_BASE.value))
        logger.debug("Patrol {} in final state".format(self.agent.name))

        await self.notify_back_in_base()

    async def run(self):
        return

class FSMPoliceBehaviour(FSMSimfleetBehaviour):
    """
        The finite state machine (FSM) that defines the behavior of the police patrol agent.

        Methods:
            setup(): Configures the FSM with states and transitions.
        """
    def setup(self):
        # Create states
        self.add_state(PoliceState.START_FROM_BASE.value, PoliceStart(), initial=True)
        self.add_state(PoliceState.NEXT_POINT.value, PoliceNextPointState())
        self.add_state(PoliceState.MOVING_TO.value, PoliceMovingToNextPointState())
        self.add_state(PoliceState.ARRIVED_POINT.value, PoliceArrivedAtPointState())
        self.add_state(PoliceState.END_ROUTE.value, PoliceEndRoute())
        self.add_state(PoliceState.BACK_INTO_BASE.value, PoliceBackIntoBase())
        self.add_state(PoliceState.EMERGENCY_CALL.value, PoliceEmergencyCallState())
        self.add_state(PoliceState.ATTEND_EMGY.value, PoliceAttendEmergencyState())

        # Route travel transitions
        self.add_transition(PoliceState.START_FROM_BASE.value, PoliceState.NEXT_POINT.value)
        self.add_transition(PoliceState.NEXT_POINT.value, PoliceState.MOVING_TO.value)
        self.add_transition(PoliceState.MOVING_TO.value, PoliceState.ARRIVED_POINT.value)
        self.add_transition(PoliceState.ARRIVED_POINT.value, PoliceState.NEXT_POINT.value)

        # Emergency notification
        self.add_transition(PoliceState.NEXT_POINT.value, PoliceState.EMERGENCY_CALL.value)
        self.add_transition(PoliceState.ARRIVED_POINT.value, PoliceState.EMERGENCY_CALL.value)
        self.add_transition(PoliceState.MOVING_TO.value, PoliceState.EMERGENCY_CALL.value)

        # Moving to emergency
        self.add_transition(PoliceState.EMERGENCY_CALL.value, PoliceState.MOVING_TO.value)
        self.add_transition(PoliceState.ARRIVED_POINT.value, PoliceState.ATTEND_EMGY.value)
        self.add_transition(PoliceState.ATTEND_EMGY.value, PoliceState.ATTEND_EMGY.value)
        self.add_transition(PoliceState.ATTEND_EMGY.value, PoliceState.MOVING_TO.value)

        # End route
        self.add_transition(PoliceState.ARRIVED_POINT.value, PoliceState.BACK_INTO_BASE.value)
