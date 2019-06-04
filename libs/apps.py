
class App():
    def __init__(self):
        pass

    @property
    def name(self):
        return 'Father'

    @property
    def app_key(self):
        return ''

class Kika(App):
    def __init__(self):
        App.__init__(self)

    @property
    def name(self):
        return 'Kika Keyboard'

    @property
    def app_key(self):
        return '78472ddd7528bcacc15725a16aeec190'

class Pro(App):
    def __init__(self):
        App.__init__(self)

    @property
    def name(self):
        return 'Emoji Keyboard'

    @property
    def app_key(self):
        return '4e5ab3a6d2140457e0423a28a094b1fd'

class iKey(App):
    def __init__(self):
        App.__init__(self)

    @property
    def name(self):
        return 'iKeyboard'

    @property
    def app_key(self):
        return 'e2934742f9d3b8ef2b59806a041ab389'

class Kika2019(App):
    def __init__(self):
        App.__init__(self)

    @property
    def name(self):
        return 'Kika Keyboard 2019'

    @property
    def app_key(self):
        return '73750b399064a5eb43afc338cd5cad25'


if __name__ == '__main__':
    kika = Kika2019()
    pass
