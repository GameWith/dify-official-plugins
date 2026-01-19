## Slack Bot

**Author:** Langgenius  
**Version:** 0.0.29
**Type:** extension

### Description

Follow these steps to integrate the Slack plugin:

1. **Create a Slack App**

   - Either create an app from a manifest or from scratch
   - Name your app and select your target workspace
     <img src="./_assets/step1.png" width="600" />
     <img src="./_assets/step2.png" width="600" />

2. **Configure App Settings**

   - Enable **Socket Mode**
   - Generate an **App-Level Token** with `connections:write` scope (starts with `xapp-`)
   - Install the app to your workspace
   - Locate your **Bot User OAuth Token** (starts with `xoxb-`)
     <img src="./_assets/step3.png" width="600" />
     <img src="./_assets/step4.png" width="600" />
     <img src="./_assets/step5.png" width="600" />

3. **Configure Event Subscriptions (No Request URL Needed)**

   - Enable **Event Subscriptions**
   - Add **Bot Events**:
     - `app_mention`
   - Add required OAuth scopes (at least):
     - `app_mentions:read`
     - `chat:write`

4. **Set Up Dify Endpoint**

   - Create a new endpoint with a custom name
   - Input your **App Token (xapp-)** and **Bot Token (xoxb-)**
   - Link to your Dify chatflow/chatbot/agent
   - Save and copy the generated endpoint URL

    <div style="display: flex; gap: 10px;">
      <img src="./_assets/step6.png" width="400" />
      <img src="./_assets/step7.png" width="400" />
    </div>

5. **Start Socket Mode Connection**

   - Open the Dify endpoint URL once (GET request) to bootstrap and start the Socket Mode connection.
   - After the connection starts, you can interact by @mentioning the bot in Slack.

6. **Final Steps**
   - Reinstall the app to your workspace if you made changes
   - Add the bot to your chosen channel
   - Start interacting by @mentioning the bot in messages
     <img src="./_assets/step11.png" width="600" />
     <img src="./_assets/step12.png" width="600" />
