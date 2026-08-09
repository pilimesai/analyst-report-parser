import sys

def guess_and_print():
    with open('app.py', 'rb') as f:
        data = f.read()

    try:
        text = data.decode('utf-8')
        print("Decoded as: utf-8")
        encoding = 'utf-8'
    except UnicodeDecodeError:
        try:
            text = data.decode('big5')
            print("Decoded as: big5")
            encoding = 'big5'
        except UnicodeDecodeError:
            try:
                text = data.decode('cp950')
                print("Decoded as: cp950")
                encoding = 'cp950'
            except UnicodeDecodeError:
                text = data.decode('utf-8', errors='replace')
                print("Decoded as: utf-8 with replacement")
                encoding = 'utf-8-replaced'
                
    for i, line in enumerate(text.split('\n')):
        print(f"{i+1:03d} | {line.replace(chr(13), '')}")

guess_and_print()
