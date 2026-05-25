document.addEventListener("DOMContentLoaded", function () {
  const socket = io();

  const form = document.getElementById("flight_delay_classification");
  const statusSpan = document.getElementById("status");
  const resultSpan = document.getElementById("result");

  socket.on("connect", function () {
    console.log("WebSocket conectado:", socket.id);
    statusSpan.textContent = "Connected";
  });

  socket.on("disconnect", function () {
    console.log("WebSocket desconectado");
    statusSpan.textContent = "Disconnected";
  });

  socket.on("kafka_response", function (message) {
    console.log("[WS RESPONSE]", message);

    statusSpan.textContent = "Response received";

    if (message.prediction !== undefined) {
      resultSpan.textContent = message.prediction;
    } else if (message.Delay !== undefined) {
      resultSpan.textContent = message.Delay;
    } else {
      resultSpan.textContent = JSON.stringify(message);
    }
  });

  form.addEventListener("submit", function (event) {
    event.preventDefault();

    statusSpan.textContent = "Sending request to Kafka...";
    resultSpan.textContent = "";

    const formData = new FormData(form);

    fetch(form.action, {
      method: "POST",
      body: formData
    })
      .then(response => response.json())
      .then(data => {
      // Registrarse en la room con el UUID
      socket.emit('register', { uuid: data.id });
      console.log("[ROOM REGISTER]", data.id);
      statusSpan.textContent = "Request sent. Waiting Kafka response...";
      })
      .catch(error => {
        console.error("Error enviando solicitud:", error);
        statusSpan.textContent = "Error sending request";
      });
  });
});