from mycroft import MycroftSkill, intent_file_handler


class MycroftTelegramBot(MycroftSkill):
    def __init__(self):
        MycroftSkill.__init__(self)

    @intent_file_handler('bot.telegram.mycroft.intent')
    def handle_bot_telegram_mycroft(self, message):
        self.speak_dialog('bot.telegram.mycroft')


def create_skill():
    return MycroftTelegramBot()

