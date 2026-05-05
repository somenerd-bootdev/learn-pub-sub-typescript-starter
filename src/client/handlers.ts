import type { ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/acktype.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
    return async (move) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ");
        if (outcome == MoveOutcome.Safe)
            return AckType.Ack;
        else if (outcome == MoveOutcome.MakeWar) {
            const rw: RecognitionOfWar = {
                attacker: move.player,
                defender: gs.getPlayerSnap(),
            };
            try {
                await publishJSON(ch, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getUsername()}`, rw);
            } catch (err) {
                return AckType.NackRequeue;
            }
            return AckType.Ack;
        }
        else {
            return AckType.NackDiscard;
        }
    };
}

export function handlerWar(gs: GameState, ch: ConfirmChannel): (rw: RecognitionOfWar) => Promise<AckType> {
    return async (rw) => {
        const outcome = handleWar(gs, rw);
        process.stdout.write("> ");
        try {
            switch (outcome.result) {
                case WarOutcome.NotInvolved:
                    return AckType.NackRequeue;
                case WarOutcome.NoUnits:
                    return AckType.NackDiscard;
                case WarOutcome.OpponentWon:
                    const oppWonMsg = `${outcome.winner} won a war against ${outcome.loser}`;
                    await publishGameLog(ch, gs.getUsername(), oppWonMsg);
                    return AckType.Ack;
                case WarOutcome.YouWon:
                    const youWonMsg = `${outcome.winner} won a war against ${outcome.loser}`; // handling this and the above as separate cases for now
                    await publishGameLog(ch, gs.getUsername(), youWonMsg);
                    return AckType.Ack;
                case WarOutcome.Draw:
                    const nobodyWonMsg = `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`;
                    await publishGameLog(ch, gs.getUsername(), nobodyWonMsg);
                    return AckType.Ack;
                default:
                    console.log("Error: War outcome unclear");
                    return AckType.NackDiscard;
            }
        } catch (err) {
            return AckType.NackRequeue;
        }
    }
}