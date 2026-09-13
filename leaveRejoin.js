// ============================================================
// leaveRejoin.js
//
// IMPORTANT CHANGE: this module used to force the bot to quit on its
// own random 1-5 minute timer, then rely on index.js to reconnect it.
// That directly violated the rotation rule "never let the old bot
// leave before its replacement has successfully joined" - a random
// self-quit has no idea whether a replacement is connecting, let
// alone whether one has joined yet. All leave/rejoin timing is now
// owned by botManager.js, which only swaps bots after a successful
// handover. So this file no longer quits the bot at all - it just
// keeps the small cosmetic "still here" jump so the bot doesn't sit
// perfectly still, and cleans itself up when the bot disconnects for
// any other reason.
// ============================================================

function randomMs(minMs, maxMs) {
    return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs
}

function setupLeaveRejoin(bot) {
    let jumpTimer = null
    let jumpOffTimer = null
    let stopped = false

    function cleanup() {
        stopped = true
        if (jumpTimer) clearTimeout(jumpTimer)
        if (jumpOffTimer) clearTimeout(jumpOffTimer)
        jumpTimer = jumpOffTimer = null
    }

    function scheduleNextJump() {
        if (stopped || !bot.entity) return

        try {
            bot.setControlState('jump', true)
            jumpOffTimer = setTimeout(() => {
                if (!stopped) {
                    try { bot.setControlState('jump', false) } catch (e) { /* ignore */ }
                }
            }, 300)
        } catch (e) {
            // bot may have just disconnected; the 'end'/'kicked' handlers below will clean up
        }

        // random jump 20s -> 5m, purely cosmetic anti-idle movement
        const nextJump = randomMs(20000, 5 * 60 * 1000)
        jumpTimer = setTimeout(scheduleNextJump, nextJump)
    }

    bot.once('spawn', () => {
        stopped = false
        scheduleNextJump()
    })

    // Reconnection/rotation is fully owned by botManager.js - this module
    // only ever cleans up its own timers when the bot goes away.
    bot.on('end', cleanup)
    bot.on('kicked', cleanup)
    bot.on('error', cleanup)
}

module.exports = setupLeaveRejoin
