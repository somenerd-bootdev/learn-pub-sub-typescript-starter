import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/acktype.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps) => {
        handlePause(gs, ps);
        console.log("> ");
        return AckType.Ack
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
    return (move) => {
        const outcome = handleMove(gs, move);
        console.log("> ");
        if (outcome != MoveOutcome.SamePlayer)
            return AckType.Ack;
        else
            return AckType.NackDiscard;
    };
}