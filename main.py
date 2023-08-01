import dash_webapp as dhwa

app = dhwa.render_app()

if __name__ == "__main__":
    app.run_server(debug=True)