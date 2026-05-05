import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void {
    return (ps) => {
        handlePause(gs, ps);
        console.log("> ");
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => void {
    return (move) => {
        handleMove(gs, move);
        console.log("> ");
    };
}