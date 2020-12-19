import tornado.ioloop
import tornado.web
from tabulate import tabulate
import DataRecipient


class ShowReport(tornado.web.RequestHandler):
    def get(self):
        data = DataRecipient.get_data_from_db()
        if data:
            columns_name = ["Date", "Client_name", "Provider_name", "Wons", "Revenue"]
            self.write(tabulate(data, tablefmt='html', showindex="always", headers=columns_name))
        else:
            self.write("There ara no data in data base!")


if __name__ == "__main__":
    DataRecipient.add_data_to_db()
    app = tornado.web.Application([
        (r"/", ShowReport)
    ])
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
