# OpenVPN Monitor

This project is a web application built with Flask that monitors multiple OpenVPN management interface profiles. It provides a user-friendly interface to view active profiles, their runtime, and IP addresses, as well as functionality to terminate specific profiles and log IP addresses associated with each profile.

## Features

- Display a table of active OpenVPN profiles with their name, runtime, and IP address.
- Ability to kill specific OpenVPN profiles.
- Log and view IP addresses used by each profile.

## Project Structure

```
openvpn-monitor
├── app
│   ├── __init__.py
│   ├── models.py
│   ├── routes.py
│   ├── openvpn.py
│   ├── utils.py
│   ├── static
│   │   ├── css
│   │   │   └── style.css
│   │   └── js
│   │       └── main.js
│   └── templates
│       ├── base.html
│       ├── index.html
│       └── profile.html
├── config.py
├── run.py
├── requirements.txt
├── logs
│   └── .gitkeep
└── README.md
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd openvpn-monitor
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure the application settings in `config.py`.

## Usage

1. Run the application:
   ```
   python run.py
   ```

2. Open your web browser and navigate to `http://127.0.0.1:5000` to access the OpenVPN monitor.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any suggestions or improvements.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.