const socket = io();

let currentUUID = null;

document.addEventListener("DOMContentLoaded", function () {
  const form = document.getElementById("flight_delay_classification");

  form.addEventListener("submit", function (event) {
    event.preventDefault();

    const url = form.getAttribute("action");
    const formData = new FormData(form);

    document.getElementById("result").textContent = "Sending request to Kafka...";

    fetch(url, {
      method: "POST",
      body: new URLSearchParams(formData) 
    })
    .then(res => res.json())
    .then(function (response) {
      if (response.status === "OK") {
        currentUUID = response.id;

        document.getElementById("result").textContent = "Processing...";

        socket.emit("register", { UUID: currentUUID });

        console.log("[ROOM REGISTER]", currentUUID);
        console.log("Waiting WebSocket response for request id " + currentUUID + "...");
      } else {
        document.getElementById("result").textContent = "Error: " + response.message;
      }
    })
    .catch(function (error) {
      console.error("Error sending request:", error);
      document.getElementById("result").textContent = "Error sending request";
    });
  });
});

socket.on("connect", function () {
  console.log("WebSocket conectado:", socket.id);
  const status = document.getElementById("status");
  if (status) status.textContent = "Connected";
});

socket.on("disconnect", function () {
  console.log("WebSocket desconectado");
  const status = document.getElementById("status");
  if (status) status.textContent = "Disconnected";
});

socket.on("kafka_response", function (message) {
  console.log("[WS RESPONSE]", message);

  const status = document.getElementById("status");
  if (status) status.textContent = "Response received";

  if (message.UUID === currentUUID || message.id === currentUUID) {
    renderPage(message);
  } else {
    console.warn("Ignoring response from another request:", message);
  }
});

function renderPage(response) {
  console.log(response);

  let prediction = response.Prediction ?? response.prediction ?? response.Delay;

  let displayMessage;

  if      (prediction == 0) displayMessage = "Early (15+ Minutes Early)";
  else if (prediction == 1) displayMessage = "Slightly Early (0-15 Minute Early)";
  else if (prediction == 2) displayMessage = "Slightly Late (0-30 Minute Delay)";
  else if (prediction == 3) displayMessage = "Very Late (30+ Minutes Late)";
  else                      displayMessage = JSON.stringify(response);

  console.log(displayMessage);

  document.getElementById("result").textContent = displayMessage;
}