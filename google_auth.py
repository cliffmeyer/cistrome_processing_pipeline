#!/usr/bin/python
import hmac, base64, struct, hashlib, time, json, os

def prefix0(h):
    """Prefixes code with leading zeros if missing."""
    if len(h) < 6:
        h = '0'*(6-len(h)) + h
    return h


def normalize(key):
    """Normalizes secret by removing spaces and padding with = to a multiple of 8"""
    k2 = key.strip().replace(' ','')
    # k2 = k2.upper()	# skipped b/c b32decode has a foldcase argument
    if len(k2)%8 != 0:
        k2 += '='*(8-len(k2)%8)
    return k2



class Validator:

    def __init__(self,key_file):
        #rel = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        #with open(os.path.join(rel,'secrets.json'), 'r') as f:
        with open(key_file,'r') as f:
            self.key_lookup = json.load(f)

	
    def get_hotp_token(self, server, intervals_no):
        """This is where the magic happens."""
        secret = self.key_lookup[server]
        key = base64.b32decode(normalize(secret), True) # True is to fold lower into uppercase
        msg = struct.pack(">Q", intervals_no)
        h = bytearray(hmac.new(key, msg, hashlib.sha1).digest())
        o = h[19] & 15
        h = str((struct.unpack(">I", h[o:o+4])[0] & 0x7fffffff) % 1000000)
        return prefix0(h)


    def get_totp_token(self, server):
        """The TOTP token is just a HOTP token seeded with every 30 seconds."""
        return self.get_hotp_token(server, intervals_no=int(time.time())//30)



def main():
    rel = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    key_file = os.path.join(rel,'google_keys.json')

    validator = Validator(key_file)
    #with open(os.path.join(rel,'secrets.json'), 'r') as f:
    #    secrets = json.load(f)
    for label, key in sorted(list(validator.key_lookup.items())):
        print("{}:\t{}".format(label, validator.get_totp_token(label)))


if __name__ == "__main__":
    main()
