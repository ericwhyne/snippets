import happybase
import base64
import urllib
import magic
import re
import datetime
import fileinput
import random, string
import dateutil.parser

# Set up a log file and the outfile
today = datetime.date.isoformat(datetime.datetime.now())
somethingrandom = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
logfilename = today + "-" + somethingrandom + "-filesync.log"
outfilename = today + "-" + somethingrandom + "-output.tsv"
logfile = open(logfilename,'w')
outfile = open(outfilename, 'w')

for line in fileinput.input():
    row = line.split(' ')
    try:
      newrow = {}
      image_s3_url = row[0]
      newrow['meta:s3_url'] = image_s3_url
      import_time = dateutil.parser.parse(row[1])

      import_time_epoch = import_time.strftime('%s')
      newrow['meta:import_time'] = import_time_epoch

      image = urllib.urlopen(image_s3_url)
      image_data = image.read()
      image_64 = base64.encodestring(image_data)
      image_mime = magic.from_buffer(image_data,mime=True)
      newrow['meta:type'] = image_mime
      newrow['image:orig'] = image_64

      rowkey = re.sub('^.*/roxyimages/','',image_s3_url)
      rowkey = re.sub('\..*$','',rowkey)

      outline = rowkey + "," + newrow['meta:import_time'] + "," + newrow['meta:type'] + "," + newrow['image:orig'] + "\n"
      outfile.write(outline)

      #htable.put(rowkey, newrow)
      logentry = "Success: " + str(rowkey) + '\n'
      logfile.write(logentry)
    except:
      logentry = "Error: " + row[0] + ' ' + row [1] + '\n'
      logfile.write(logentry)

logfile.close()
outfile.close()
