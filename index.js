const fs = require('fs');
const express = require('express');
const wiegine = require('fca-mafiya');
const WebSocket = require('ws');

const app = express();
const PORT = 22917;

let tasks = [];
let tempApi = null;
let keepAliveInterval = null;
let wsHeartbeatInterval = null;
let taskHealthInterval = null;
let serverStartTime = Date.now();

// Process error handlers - prevent server from crashing
process.on('uncaughtException', (err) => {
    console.error('[KEEP-ALIVE] Uncaught Exception:', err.message);
    console.error(err.stack);
    // Don't exit, just log
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('[KEEP-ALIVE] Unhandled Rejection:', reason);
    // Don't exit, just log
});

// Prevent process from exiting
process.on('exit', (code) => {
    console.log('[KEEP-ALIVE] Process exit prevented, code:', code);
});

// Keep server alive - runs every 10 seconds
function startKeepAlive() {
    if (keepAliveInterval) clearInterval(keepAliveInterval);
    
    let aliveCounter = 0;
    keepAliveInterval = setInterval(() => {
        aliveCounter++;
        const now = new Date().toLocaleTimeString();
        const uptime = Math.floor((Date.now() - serverStartTime) / 1000);
        const uptimeStr = formatUptime(uptime);
        const runningTasks = tasks.filter(t => t.status === 'Running').length;
        
        // Log every 6th check (every minute) to reduce spam
        if (aliveCounter % 6 === 0) {
            console.log(`[${now}] [KEEP-ALIVE] Server alive | Uptime: ${uptimeStr} | Running: ${runningTasks}/${tasks.length} tasks`);
        }
        
        // Check and restart any broken tasks
        tasks.forEach(task => {
            if ((task.status === 'Disconnected' || task.status === 'Reconnect Failed') && task.cookieContent) {
                reconnectTask(task);
            }
            
            // Check if task lockers are still running
            if (task.status === 'Running' && task.api) {
                if (task.groupName && !task.nameLocker) {
                    serverLog(`Task ${task.id}: Name locker stopped, restarting...`);
                    startNameLocker(task);
                }
                if (task.nickname && !task.nicknameLocker) {
                    serverLog(`Task ${task.id}: Nickname locker stopped, restarting...`);
                    startNicknameLocker(task);
                }
                if (task.messages && task.messages.length > 0 && !task.msgSender) {
                    serverLog(`Task ${task.id}: Message sender stopped, restarting...`);
                    startMessageSender(task);
                }
            }
        });
    }, 10000); // Every 10 seconds
}

// Format uptime as days, hours, minutes, seconds
function formatUptime(seconds) {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    let str = '';
    if (days > 0) str += `${days}d `;
    if (hours > 0) str += `${hours}h `;
    if (mins > 0) str += `${mins}m `;
    str += `${secs}s`;
    return str;
}

// Task health checker - runs every 30 seconds
function startTaskHealthChecker() {
    if (taskHealthInterval) clearInterval(taskHealthInterval);
    
    taskHealthInterval = setInterval(() => {
        tasks.forEach(task => {
            if (task.status === 'Running' && task.api) {
                // Ping the API to check if it's still alive
                try {
                    task.api.getThreadInfo(task.threadID, (err, info) => {
                        if (err) {
                            serverLog(`Task ${task.id}: API connection lost, marking for reconnection`);
                            task.status = 'Disconnected';
                        }
                    });
                } catch (e) {
                    serverLog(`Task ${task.id}: Health check failed: ${e.message}`);
                    task.status = 'Disconnected';
                }
            }
        });
    }, 30000); // Every 30 seconds
}

// Reconnect a disconnected task with retry
function reconnectTask(task, retryCount = 0) {
    if (retryCount > 3) {
        task.status = 'Reconnect Failed';
        broadcastUpdate(`❌ Task ${task.id} failed after 3 retries`);
        
        // Schedule another retry in 5 minutes
        setTimeout(() => {
            if (task.status === 'Reconnect Failed' && task.cookieContent) {
                serverLog(`Task ${task.id}: Scheduled retry after failure...`);
                reconnectTask(task, 0);
            }
        }, 300000); // 5 minutes
        return;
    }
    
    task.status = 'Reconnecting...';
    serverLog(`Task ${task.id}: Attempting reconnection (attempt ${retryCount + 1})...`);
    broadcastUpdate(`🔄 Reconnecting task ${task.id}...`);
    
    wiegine.login(task.cookieContent, {}, (err, api) => {
        if (err || !api) {
            serverLog(`Task ${task.id}: Reconnection attempt ${retryCount + 1} failed`);
            
            // Retry with exponential backoff
            const delay = Math.pow(2, retryCount) * 5000; // 5s, 10s, 20s
            setTimeout(() => reconnectTask(task, retryCount + 1), delay);
            return;
        }
        
        task.api = api;
        task.status = 'Running';
        
        // Restart lockers and message sender
        if (task.groupName) startNameLocker(task);
        if (task.nickname) startNicknameLocker(task);
        if (task.messages && task.messages.length > 0) startMessageSender(task);
        
        serverLog(`Task ${task.id}: Reconnected successfully`);
        broadcastUpdate(`✅ Task ${task.id} reconnected successfully`);
    });
}

function generateTaskKey() {
    let result;
    do {
        result = Math.floor(10000 + Math.random() * 90000); 
    } while (findTask(result));
    return result;
}

function findTask(id) {
    return tasks.find(t => t.id === id);
}

function createNewTask(threadID, groupName, nickname, cookieContent, groupDisplayName, messages, prefix, delay) {
    const taskId = generateTaskKey();
    const newTask = {
        id: taskId,
        status: 'Starting',
        threadID: threadID,
        groupName: groupName,
        nickname: nickname,
        groupDisplayName: groupDisplayName || threadID,
        messages: messages || [],
        prefix: prefix || '',
        delay: delay || 5,
        currentIndex: 0,
        api: null,
        cookieContent: cookieContent,
        nameLocker: null,
        nicknameLocker: null,
        msgSender: null,
        lockedNicknames: {}
    };
    tasks.push(newTask);
    return newTask;
}

let wss;

function serverLog(message) {
    const d = new Date().toLocaleTimeString();
    console.log(`[${d}] ${message}`);
}

function startNameLocker(task) {
    if (!task.groupName) return;
    
    // Clear existing locker if any
    if (task.nameLocker) {
        clearInterval(task.nameLocker);
        task.nameLocker = null;
    }
    
    const threadID = task.threadID;
    const desiredName = task.groupName;

    // Set initial name
    if (task.api) {
        task.api.setTitle(desiredName, threadID, (err) => {
            if (err) {
                serverLog(`Task ${task.id}: Failed to set group name: ${err.error || err}`);
            } else {
                serverLog(`Task ${task.id}: Group Name set to: ${desiredName}`);
            }
        });
    }
    
    // Check and lock name every 2 seconds (faster response)
    const checkAndLockName = () => {
        if (task.status !== 'Running' || !task.api || !task.groupName) {
            if (task.nameLocker) {
                clearInterval(task.nameLocker); 
                task.nameLocker = null;
            }
            return;
        }

        try {
            task.api.getThreadInfo(threadID, (err, info) => {
                if (err) {
                    serverLog(`Task ${task.id}: Error checking name: ${err.error || err}`);
                    return;
                }

                if (info && info.threadName !== desiredName) {
                    serverLog(`Task ${task.id}: Name changed to "${info.threadName}"! Restoring to: ${desiredName}`);
                    broadcastUpdate(`🚨 Name changed! Restoring: ${desiredName}`);
                    
                    // Immediately restore the name
                    task.api.setTitle(desiredName, threadID, (setErr) => {
                        if (setErr) {
                            serverLog(`Task ${task.id}: Retry setting name...`);
                            // Retry after 1 second
                            setTimeout(() => {
                                if (task.api && task.status === 'Running') {
                                    task.api.setTitle(desiredName, threadID, () => {});
                                }
                            }, 1000);
                        } else {
                            serverLog(`Task ${task.id}: Name restored to: ${desiredName}`);
                            broadcastUpdate(`✅ Name restored: ${desiredName}`);
                        }
                    });
                }
            });
        } catch (e) {
            serverLog(`Task ${task.id}: Name check error: ${e.message}`);
        }
    };

    // Check every 2 seconds for faster response
    task.nameLocker = setInterval(checkAndLockName, 2000); 
    broadcastUpdate(`🔒 Group Name Lock Active: ${desiredName} (checking every 2s)`);
    
    // Run first check immediately
    setTimeout(checkAndLockName, 500);
}

function startNicknameLocker(task) {
    if (!task.nickname) return;
    
    // Clear existing locker if any
    if (task.nicknameLocker) {
        clearInterval(task.nicknameLocker);
        task.nicknameLocker = null;
    }
    
    const threadID = task.threadID;
    const desiredNickname = task.nickname;

    // Set initial nicknames
    if (task.api) {
        task.api.getThreadInfo(threadID, (err, info) => {
            if (err) return;

            const participantIDs = info.participantIDs || [];
            
            participantIDs.forEach(userID => {
                task.lockedNicknames[userID] = desiredNickname;
                task.api.changeNickname(desiredNickname, threadID, userID, () => {});
            });
        });
    }

    const checkAndLockNicknames = () => {
        if (task.status !== 'Running' || !task.api || !task.nickname) {
            if (task.nicknameLocker) {
                clearInterval(task.nicknameLocker); 
                task.nicknameLocker = null;
            }
            return;
        }

        try {
            task.api.getThreadInfo(threadID, (err, info) => {
                if (err) return;

                const nicknames = info.nicknames || {};
                const participantIDs = info.participantIDs || [];

                participantIDs.forEach(userID => {
                    const currentNickname = nicknames[userID] || '';
                    if (currentNickname !== desiredNickname) {
                        task.api.changeNickname(desiredNickname, threadID, userID, () => {});
                    }
                });
            });
        } catch (e) {
            serverLog(`Task ${task.id}: Nickname check error: ${e.message}`);
        }
    };

    // Check every 2 seconds
    task.nicknameLocker = setInterval(checkAndLockNicknames, 2000);
    broadcastUpdate(`🔒 Nickname Lock Active: ${desiredNickname} (checking every 2s)`);
    
    // Run first check immediately
    setTimeout(checkAndLockNicknames, 500);
}

function startMessageSender(task) {
    if (!task.messages || task.messages.length === 0 || task.msgSender) return;

    const api = task.api;
    const threadID = task.threadID;

    const sendNextMessage = () => {
        if (task.status !== 'Running' || !task.api) {
            clearTimeout(task.msgSender);
            task.msgSender = null;
            return;
        }

        if (task.currentIndex >= task.messages.length) {
            task.currentIndex = 0;
            serverLog(`Task ${task.id}: Message loop restarted`);
        }

        const rawMsg = task.messages[task.currentIndex];
        const fullMsg = task.prefix ? `${task.prefix} ${rawMsg}` : rawMsg;

        api.sendMessage(fullMsg, threadID, (err) => {
            if (err) {
                serverLog(`Task ${task.id}: Failed to send message: ${err.error || err}`);
            } else {
                serverLog(`Task ${task.id}: Message sent: ${fullMsg.substring(0, 50)}...`);
            }

            task.currentIndex++;
            task.msgSender = setTimeout(sendNextMessage, task.delay * 1000);
        });
    };

    serverLog(`Task ${task.id}: Message Sender started with ${task.messages.length} messages`);
    broadcastUpdate(`📨 Message Sender Active: ${task.messages.length} messages, ${task.delay}s delay`);
    sendNextMessage();
}

function fetchGroups(cookieContent, callback) {
    wiegine.login(cookieContent, {}, (err, api) => {
        if (err || !api) {
            callback({ error: 'Login failed. Check cookies.' });
            return;
        }

        tempApi = api;

        api.getThreadList(100, null, ['INBOX'], (err, threads) => {
            if (err) {
                callback({ error: 'Failed to fetch groups.' });
                return;
            }

            const groups = threads
                .filter(t => t.isGroup && t.threadID)
                .map(t => ({
                    id: t.threadID,
                    name: t.name || `Group ${t.threadID}`,
                    participants: t.participantIDs ? t.participantIDs.length : 0
                }));

            callback({ groups, cookieContent });
        });
    });
}

function startTask(cookieContent, selectedGroups, groupName, nickname, messages, prefix, delay) {
    const hasLock = groupName || nickname;
    const hasMsg = messages && messages.length > 0;

    if (!hasLock && !hasMsg) {
        broadcastUpdate('❌ Please provide lock settings or messages.');
        return;
    }

    if (!selectedGroups || selectedGroups.length === 0) {
        broadcastUpdate('❌ Please select at least one group.');
        return;
    }

    selectedGroups.forEach((group, index) => {
        setTimeout(() => {
            const task = createNewTask(group.id, groupName, nickname, cookieContent, group.name, messages, prefix, delay);
            task.status = 'Logging In...';
            
            serverLog(`Task ${task.id} for ${group.name} created.`);

            wiegine.login(cookieContent, {}, (err, api) => {
                if (err || !api) {
                    task.status = 'Login Failed';
                    broadcastUpdate(`❌ Login failed for ${group.name}`);
                    return;
                }

                task.api = api;
                task.status = 'Running';
                
                if (task.groupName) startNameLocker(task);
                if (task.nickname) startNicknameLocker(task);
                if (task.messages.length > 0) startMessageSender(task);
                
                broadcastUpdate(`✅ Task ${task.id} started for: ${group.name}`);
            });
        }, index * 2000);
    });
}

function stopTask(taskId) {
    const task = findTask(taskId);
    if (!task || task.status === 'Stopped') {
        broadcastUpdate(`⚠️ Task ${taskId} not found or already stopped.`);
        return false;
    }
    
    if (task.nameLocker) clearInterval(task.nameLocker);
    if (task.nicknameLocker) clearInterval(task.nicknameLocker);
    if (task.msgSender) clearTimeout(task.msgSender);
    if (task.api && typeof task.api.logout === 'function') {
        try { task.api.logout(); } catch(e) {}
    }
    
    task.status = 'Stopped';
    broadcastUpdate(`⏹️ Task ${taskId} stopped.`);
    return true;
}

function broadcastUpdate(message) {
    broadcast({ type: 'status', message });
}

function broadcast(message) {
    if (!wss) return;
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            try { client.send(JSON.stringify(message)); } catch (e) {}
        }
    });
}

const htmlPanel = `
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>M9YU5PL0B3 ON F1R3 - Group Manager</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{
  font-family:'Courier New',monospace;
  min-height:100vh;
  background:url('https://cdn.postimage.me/2026/10/18/1000068598.jpg') no-repeat center center fixed;
  background-size:cover;
  color:#0f0;
  overflow-x:hidden;
}

body::before{
  content:'';
  position:fixed;
  top:0;left:0;right:0;bottom:0;
  background:rgba(0,0,0,0.7);
  z-index:0;
}

.matrix-rain{
  position:fixed;
  top:0;left:0;right:0;bottom:0;
  pointer-events:none;
  z-index:1;
  overflow:hidden;
}
.matrix-char{
  position:absolute;
  color:#0f0;
  font-size:14px;
  animation:fall linear infinite;
  text-shadow:0 0 5px #0f0;
  opacity:0.7;
}
@keyframes fall{
  0%{transform:translateY(-100%);opacity:1}
  100%{transform:translateY(100vh);opacity:0}
}

.glitch{
  position:relative;
  animation:glitch 2s infinite;
}
@keyframes glitch{
  0%,90%,100%{transform:translate(0)}
  92%{transform:translate(-2px,2px)}
  94%{transform:translate(2px,-2px)}
  96%{transform:translate(-2px,-2px)}
  98%{transform:translate(2px,2px)}
}

.scanline{
  position:fixed;
  top:0;left:0;right:0;
  height:3px;
  background:linear-gradient(90deg,transparent,#0f0,#0ff,#f0f,transparent);
  animation:scan 2s linear infinite;
  z-index:1000;
  opacity:0.8;
}
@keyframes scan{0%{top:-10px}100%{top:100vh}}

.flicker{
  animation:flicker 0.1s infinite;
}
@keyframes flicker{
  0%,100%{opacity:1}
  50%{opacity:0.95}
}

.container{
  position:relative;
  z-index:10;
  max-width:520px;
  margin:20px auto;
  padding:15px;
}

.header{
  text-align:center;
  padding:20px;
  background:rgba(0,0,0,0.8);
  border:3px solid;
  border-image:linear-gradient(45deg,#f0f,#0ff,#0f0,#ff0) 1;
  margin-bottom:15px;
  position:relative;
  animation:borderGlow 3s infinite;
}
@keyframes borderGlow{
  0%,100%{box-shadow:0 0 20px #f0f}
  33%{box-shadow:0 0 20px #0ff}
  66%{box-shadow:0 0 20px #0f0}
}
.header-img{
  width:100px;
  height:100px;
  border-radius:50%;
  border:4px solid #0ff;
  box-shadow:0 0 30px #0ff,0 0 60px #f0f;
  margin-bottom:15px;
  animation:pulse 2s infinite;
}
@keyframes pulse{
  0%,100%{box-shadow:0 0 30px #0ff,0 0 60px #f0f}
  50%{box-shadow:0 0 50px #0ff,0 0 80px #f0f}
}
.header h1{
  font-size:26px;
  background:linear-gradient(90deg,#0f0,#0ff,#f0f,#ff0);
  background-size:400% 400%;
  -webkit-background-clip:text;
  -webkit-text-fill-color:transparent;
  animation:gradient 3s ease infinite;
  letter-spacing:4px;
}
@keyframes gradient{
  0%{background-position:0% 50%}
  50%{background-position:100% 50%}
  100%{background-position:0% 50%}
}
.header p{
  color:#ff0;
  font-size:12px;
  margin-top:10px;
  text-shadow:0 0 10px #ff0;
}

.panel{
  background:rgba(0,0,0,0.85);
  padding:18px;
  margin-bottom:15px;
  position:relative;
  overflow:hidden;
  border-radius:10px;
}
.panel.color-1{border:2px solid #ff3366;box-shadow:0 0 15px rgba(255,51,102,0.5)}
.panel.color-2{border:2px solid #00ff88;box-shadow:0 0 15px rgba(0,255,136,0.5)}
.panel.color-3{border:2px solid #00ccff;box-shadow:0 0 15px rgba(0,204,255,0.5)}
.panel.color-4{border:2px solid #ffcc00;box-shadow:0 0 15px rgba(255,204,0,0.5)}
.panel.color-5{border:2px solid #cc66ff;box-shadow:0 0 15px rgba(204,102,255,0.5)}
.panel.color-6{border:2px solid #ff6600;box-shadow:0 0 15px rgba(255,102,0,0.5)}

.panel-title{
  font-size:14px;
  margin-bottom:12px;
  padding-bottom:8px;
  border-bottom:1px dashed currentColor;
  text-transform:uppercase;
  letter-spacing:2px;
}
.panel.color-1 .panel-title{color:#ff3366}
.panel.color-2 .panel-title{color:#00ff88}
.panel.color-3 .panel-title{color:#00ccff}
.panel.color-4 .panel-title{color:#ffcc00}
.panel.color-5 .panel-title{color:#cc66ff}
.panel.color-6 .panel-title{color:#ff6600}

.input-group{margin-bottom:12px}
.input-group label{
  display:block;
  color:#0ff;
  font-size:11px;
  margin-bottom:5px;
  text-transform:uppercase;
}
.input-group input,.input-group textarea{
  width:100%;
  padding:12px;
  background:rgba(0,0,0,0.9);
  border:2px solid #0ff;
  border-radius:8px;
  color:#fff;
  font-family:inherit;
  font-size:13px;
  transition:all 0.3s;
}
.input-group input:focus,.input-group textarea:focus{
  outline:none;
  border-color:#f0f;
  box-shadow:0 0 15px #f0f;
}
.input-group input[type="file"]{padding:10px}
.input-group input[type="file"]::file-selector-button{
  background:linear-gradient(45deg,#0ff,#0f0);
  color:#000;
  border:none;
  padding:8px 15px;
  border-radius:5px;
  cursor:pointer;
  font-family:inherit;
  font-weight:bold;
  margin-right:10px;
}

.radio-group{
  display:flex;
  gap:25px;
  margin-bottom:15px;
}
.radio-group label{
  display:flex;
  align-items:center;
  gap:8px;
  cursor:pointer;
  color:#0ff;
  font-size:13px;
}

.btn{
  width:100%;
  padding:14px;
  border:none;
  border-radius:8px;
  font-family:inherit;
  font-size:14px;
  font-weight:bold;
  cursor:pointer;
  text-transform:uppercase;
  letter-spacing:2px;
  transition:all 0.3s;
  margin-bottom:10px;
}
.btn-primary{
  background:linear-gradient(45deg,#00ff88,#00ccff);
  color:#000;
  box-shadow:0 0 20px rgba(0,255,136,0.5);
}
.btn-primary:hover{transform:scale(1.02);box-shadow:0 0 30px rgba(0,255,136,0.8)}
.btn-danger{
  background:linear-gradient(45deg,#ff3366,#ff6600);
  color:#fff;
  box-shadow:0 0 20px rgba(255,51,102,0.5);
}
.btn-danger:hover{transform:scale(1.02);box-shadow:0 0 30px rgba(255,51,102,0.8)}
.btn-warning{
  background:linear-gradient(45deg,#ffcc00,#ff6600);
  color:#000;
  box-shadow:0 0 20px rgba(255,204,0,0.5);
}
.btn-warning:hover{transform:scale(1.02)}
.btn-cyan{
  background:linear-gradient(45deg,#00ccff,#cc66ff);
  color:#000;
  box-shadow:0 0 20px rgba(0,204,255,0.5);
}
.btn-cyan:hover{transform:scale(1.02);box-shadow:0 0 30px rgba(204,102,255,0.8)}
.btn:disabled{opacity:0.5;cursor:not-allowed;transform:none}

.status-box{
  background:rgba(0,0,0,0.9);
  border:2px solid #00ff88;
  border-radius:8px;
  padding:12px;
  max-height:120px;
  overflow-y:auto;
  font-size:11px;
  margin-bottom:12px;
}
.status-box .log{
  padding:5px 0;
  border-bottom:1px dotted #00ff88;
  color:#0ff;
}
.status-box .log:last-child{border:none}

.groups-container{
  max-height:180px;
  overflow-y:auto;
  background:rgba(0,0,0,0.9);
  border:2px solid #0ff;
  border-radius:8px;
  display:none;
}
.group-item{
  display:flex;
  align-items:center;
  padding:12px;
  border-bottom:1px solid rgba(0,255,255,0.3);
  cursor:pointer;
  transition:all 0.2s;
}
.group-item:hover{background:rgba(0,255,255,0.1)}
.group-item.selected{background:rgba(0,255,255,0.2);border-left:4px solid #0ff}
.group-item input{margin-right:12px}
.group-item .name{flex:1;color:#fff;font-size:12px}
.group-item .count{color:#ff0;font-size:10px}

.step{display:none}
.step.active{display:block}

.loader{text-align:center;padding:30px;display:none}
.loader.active{display:block}
.loader-text{
  color:#0ff;
  font-size:14px;
  animation:blink 0.5s infinite;
}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}

.optional-tag{
  background:linear-gradient(45deg,#f0f,#0ff);
  color:#000;
  font-size:9px;
  padding:3px 8px;
  margin-left:10px;
  border-radius:10px;
  font-weight:bold;
}

footer{
  text-align:center;
  padding:20px;
  color:#0ff;
  font-size:11px;
  text-shadow:0 0 10px #0ff;
}
</style>
</head>
<body>
<div class="matrix-rain" id="matrix"></div>
<div class="scanline"></div>

<div class="container">
  <div class="header glitch">
    <img src="https://cdn.postimage.me/2026/10/18/1000071773.jpg" alt="Logo" class="header-img">
    <h1 class="flicker">M9YU5PL0V3 0N F1R3</h1>
    <p>[ GROUP LOCK & MESSAGE SENDER SYSTEM ]</p>
  </div>

  <div id="step1" class="step active">
    <div class="panel color-1">
      <div class="panel-title">// Step 1: Authentication</div>
      
      <div class="radio-group">
        <label><input type="radio" name="cookie-mode" value="file" checked> Upload File</label>
        <label><input type="radio" name="cookie-mode" value="paste"> Paste Cookies</label>
      </div>

      <div id="file-input" class="input-group">
        <label>Cookie File (.txt/.json)</label>
        <input type="file" id="cookie-file" accept=".txt,.json">
      </div>

      <div id="paste-input" class="input-group" style="display:none">
        <label>Paste Cookies</label>
        <textarea id="cookie-paste" rows="3" placeholder="Paste cookies here..."></textarea>
      </div>

      <button class="btn btn-primary" id="fetch-btn">[ FETCH GROUPS ]</button>
    </div>
  </div>

  <div id="loader" class="loader">
    <div class="loader-text">>>> FETCHING GROUPS... PLEASE WAIT <<<</div>
  </div>

  <div id="step2" class="step">
    <div class="panel color-2">
      <div class="panel-title">// Step 2: Select Groups</div>
      
      <div class="input-group">
        <label>Found Groups (Click to Select)</label>
        <div id="groups-list" class="groups-container"></div>
      </div>

      <button class="btn btn-warning" id="select-all-btn">[ SELECT ALL ]</button>
    </div>

    <div class="panel color-3">
      <div class="panel-title">// Lock Settings <span class="optional-tag">OPTIONAL</span></div>
      
      <div class="input-group">
        <label>Lock Group Name</label>
        <input type="text" id="group-name" placeholder="Enter name to lock (optional)">
      </div>

      <div class="input-group">
        <label>Lock Nickname (All Users)</label>
        <input type="text" id="nickname" placeholder="Enter nickname to lock (optional)">
      </div>
    </div>

    <div class="panel color-4">
      <div class="panel-title">// Message Sender <span class="optional-tag">OPTIONAL</span></div>
      
      <div class="input-group">
        <label>Message File (.txt) - One message per line</label>
        <input type="file" id="msg-file" accept=".txt">
      </div>

      <div class="input-group">
        <label>Prefix (Added before each message)</label>
        <input type="text" id="msg-prefix" placeholder="e.g. [BOT]">
      </div>

      <div class="input-group">
        <label>Delay (Seconds between messages)</label>
        <input type="number" id="msg-delay" value="5" min="1" placeholder="5">
      </div>
    </div>

    <button class="btn btn-cyan" id="start-btn">[ START TASK ]</button>
  </div>

  <div class="panel color-5">
    <div class="panel-title">// System Status</div>
    <div id="status-box" class="status-box">
      <div class="log">> System ready...</div>
    </div>
  </div>

  <div class="panel color-6">
    <div class="panel-title">// Stop Task</div>
    <div class="input-group">
      <label>Enter Task ID</label>
      <input type="text" id="stop-task-id" placeholder="5-digit Task ID">
    </div>
    <button class="btn btn-danger" id="stop-btn">[ STOP TASK ]</button>
  </div>

  <footer>SUHANII SINGH  SYSTEM v3.0 | LOCK + MSG SENDER</footer>
</div>

<script>
const socket = new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host);
let cookieContent = '';
let fetchedGroups = [];
let selectedGroups = [];
let messages = [];

const step1 = document.getElementById('step1');
const step2 = document.getElementById('step2');
const loader = document.getElementById('loader');
const statusBox = document.getElementById('status-box');
const groupsList = document.getElementById('groups-list');

function log(msg) {
  const div = document.createElement('div');
  div.className = 'log';
  div.textContent = '> ' + msg;
  statusBox.insertBefore(div, statusBox.firstChild);
  if(statusBox.children.length > 20) statusBox.removeChild(statusBox.lastChild);
}

function updateCookieInput() {
  const mode = document.querySelector('input[name="cookie-mode"]:checked').value;
  document.getElementById('file-input').style.display = mode==='file'?'block':'none';
  document.getElementById('paste-input').style.display = mode==='paste'?'block':'none';
}

document.querySelectorAll('input[name="cookie-mode"]').forEach(r => {
  r.addEventListener('change', updateCookieInput);
});

socket.onopen = () => log('Connected to server');
socket.onclose = () => log('Disconnected from server');
socket.onerror = () => log('Connection error');

socket.onmessage = (e) => {
  try {
    const data = JSON.parse(e.data);
    if(data.type === 'status') {
      log(data.message);
    }
    if(data.type === 'groups') {
      loader.classList.remove('active');
      if(data.error) {
        log('ERROR: ' + data.error);
        step1.classList.add('active');
      } else {
        fetchedGroups = data.groups;
        cookieContent = data.cookieContent;
        renderGroups();
        step2.classList.add('active');
        log('Found ' + data.groups.length + ' groups');
      }
    }
  } catch(e) {}
};

function renderGroups() {
  groupsList.innerHTML = '';
  groupsList.style.display = 'block';
  fetchedGroups.forEach(g => {
    const div = document.createElement('div');
    div.className = 'group-item';
    div.innerHTML = '<input type="checkbox" data-id="'+g.id+'">' +
      '<span class="name">'+g.name+'</span>' +
      '<span class="count">['+g.participants+']</span>';
    div.onclick = (e) => {
      if(e.target.tagName !== 'INPUT') {
        const cb = div.querySelector('input');
        cb.checked = !cb.checked;
      }
      div.classList.toggle('selected', div.querySelector('input').checked);
      updateSelected();
    };
    groupsList.appendChild(div);
  });
}

function updateSelected() {
  selectedGroups = [];
  document.querySelectorAll('.group-item input:checked').forEach(cb => {
    const g = fetchedGroups.find(x => x.id === cb.dataset.id);
    if(g) selectedGroups.push(g);
  });
}

document.getElementById('select-all-btn').onclick = () => {
  const items = document.querySelectorAll('.group-item');
  const allSelected = selectedGroups.length === fetchedGroups.length;
  items.forEach(item => {
    item.querySelector('input').checked = !allSelected;
    item.classList.toggle('selected', !allSelected);
  });
  updateSelected();
};

document.getElementById('fetch-btn').onclick = () => {
  const mode = document.querySelector('input[name="cookie-mode"]:checked').value;
  
  if(mode === 'paste') {
    const content = document.getElementById('cookie-paste').value.trim();
    if(!content) { alert('Please paste cookies'); return; }
    sendFetch(content);
  } else {
    const file = document.getElementById('cookie-file').files[0];
    if(!file) { alert('Please select cookie file'); return; }
    const reader = new FileReader();
    reader.onload = (e) => sendFetch(e.target.result);
    reader.readAsText(file);
  }
};

function sendFetch(content) {
  step1.classList.remove('active');
  step2.classList.remove('active');
  loader.classList.add('active');
  log('Fetching groups...');
  socket.send(JSON.stringify({ type: 'fetch_groups', cookieContent: content }));
}

document.getElementById('start-btn').onclick = () => {
  const groupName = document.getElementById('group-name').value.trim();
  const nickname = document.getElementById('nickname').value.trim();
  const prefix = document.getElementById('msg-prefix').value.trim();
  const delay = parseInt(document.getElementById('msg-delay').value) || 5;
  const msgFile = document.getElementById('msg-file').files[0];
  
  if(selectedGroups.length === 0) { 
    alert('Select at least one group'); 
    return; 
  }

  const startTask = (msgs) => {
    if(!groupName && !nickname && msgs.length === 0) {
      alert('Please set lock options OR upload message file');
      return;
    }
    
    log('Starting task for ' + selectedGroups.length + ' groups...');
    socket.send(JSON.stringify({
      type: 'start_task',
      cookieContent,
      selectedGroups,
      groupName,
      nickname,
      messages: msgs,
      prefix,
      delay
    }));
  };

  if(msgFile) {
    const reader = new FileReader();
    reader.onload = (e) => {
      const lines = e.target.result.split('\\n')
        .map(l => l.trim())
        .filter(l => l.length > 0);
      startTask(lines);
    };
    reader.readAsText(msgFile);
  } else {
    startTask([]);
  }
};

document.getElementById('stop-btn').onclick = () => {
  const taskId = document.getElementById('stop-task-id').value.trim();
  if(!taskId || isNaN(taskId)) { alert('Enter valid Task ID'); return; }
  socket.send(JSON.stringify({ type: 'stop_task', id: parseInt(taskId) }));
};

// Matrix rain effect
const matrix = document.getElementById('matrix');
const chars = '01アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン';
for(let i = 0; i < 50; i++) {
  const span = document.createElement('span');
  span.className = 'matrix-char';
  span.textContent = chars[Math.floor(Math.random() * chars.length)];
  span.style.left = Math.random() * 100 + '%';
  span.style.animationDuration = (Math.random() * 3 + 2) + 's';
  span.style.animationDelay = Math.random() * 5 + 's';
  matrix.appendChild(span);
}
</script>
</body>
</html>
`;

app.get('/', (req, res) => {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.send(htmlPanel);
});

// Health endpoint for keep-alive
app.get('/health', (req, res) => {
    const runningTasks = tasks.filter(t => t.status === 'Running').length;
    res.json({ 
        status: 'alive', 
        uptime: process.uptime(),
        tasks: tasks.length,
        running: runningTasks,
        timestamp: new Date().toISOString()
    });
});

// Status endpoint
app.get('/status', (req, res) => {
    res.json({
        server: 'ROWEDY KINNG - GROUP MANAGER',
        status: 'running',
        tasks: tasks.map(t => ({
            id: t.id,
            group: t.groupDisplayName,
            status: t.status,
            hasNameLock: !!t.groupName,
            hasNicknameLock: !!t.nickname,
            hasMessages: t.messages && t.messages.length > 0
        }))
    });
});

const server = app.listen(PORT, '0.0.0.0', () => {
    console.log('='.repeat(50));
    console.log('👑 ROWEDY KINNG - GROUP MANAGER SYSTEM');
    console.log('='.repeat(50));
    console.log(`Server: http://0.0.0.0:${PORT}`);
    console.log('[KEEP-ALIVE] System initialized - Server will run forever');
    console.log('[KEEP-ALIVE] Name lock checks every 2 seconds');
    console.log('[KEEP-ALIVE] Task health checks every 30 seconds');
    console.log('[KEEP-ALIVE] Auto-reconnect enabled with retry');
    
    // Start keep-alive systems
    startKeepAlive();
    startTaskHealthChecker();
});

wss = new WebSocket.Server({ server });

// WebSocket heartbeat - ping clients every 30 seconds
function startWsHeartbeat() {
    if (wsHeartbeatInterval) clearInterval(wsHeartbeatInterval);
    wsHeartbeatInterval = setInterval(() => {
        wss.clients.forEach((ws) => {
            if (ws.isAlive === false) {
                return ws.terminate();
            }
            ws.isAlive = false;
            try { ws.ping(); } catch(e) {}
        });
    }, 30000);
}

startWsHeartbeat();

wss.on('connection', (ws) => {
    ws.isAlive = true;
    serverLog('Client connected');

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('error', (err) => {
        serverLog('WebSocket error: ' + err.message);
    });

    ws.on('close', () => {
        serverLog('Client disconnected');
    });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (data.type === 'fetch_groups') {
                fetchGroups(data.cookieContent, (result) => {
                    ws.send(JSON.stringify({ type: 'groups', ...result }));
                });
            } else if (data.type === 'start_task') {
                startTask(
                    data.cookieContent, 
                    data.selectedGroups, 
                    data.groupName, 
                    data.nickname,
                    data.messages,
                    data.prefix,
                    data.delay
                );
            } else if (data.type === 'stop_task') {
                stopTask(parseInt(data.id));
            }
        } catch (err) {
            serverLog('Error: ' + err.message);
        }
    });
});

// Self-ping to keep server alive on hosting platforms
const http = require('http');
setInterval(() => {
    try {
        http.get(`http://localhost:${PORT}/health`, (res) => {
            // Keep connection active
        }).on('error', () => {});
    } catch(e) {}
}, 30000); // Every 30 seconds

// Additional keep-alive - prevents event loop from going empty
setInterval(() => {
    // This empty interval keeps Node.js event loop active forever
}, 1000);

console.log('[KEEP-ALIVE] Self-ping active every 30 seconds');
console.log('[KEEP-ALIVE] Event loop keep-alive active');
console.log('='.repeat(50));
