const sourceSelect = document.getElementById("source");
const clickhouseConfig = document.getElementById("clickhouse-config");
const flatfileConfig = document.getElementById("flatfile-config");
const connectBtn = document.getElementById("connectBtn");
const loadColumnsBtn = document.getElementById("loadColumnsBtn");
const startBtn = document.getElementById("startIngestionBtn");
const statusText = document.getElementById("statusText");
const resultText = document.getElementById("resultText");
const columnsContainer = document.getElementById("columnsContainer");
const columnsSection = document.getElementById("columns-section");

let selectedColumns = [];

sourceSelect.addEventListener("change", () => {
  if (sourceSelect.value === "clickhouse") {
    clickhouseConfig.style.display = "block";
    flatfileConfig.style.display = "none";
  } else {
    clickhouseConfig.style.display = "none";
    flatfileConfig.style.display = "block";
  }
});

connectBtn.addEventListener("click", () => {
  statusText.textContent = "Connecting...";
  // You can implement backend validation if needed
  setTimeout(() => {
    statusText.textContent = "Connected!";
  }, 1000);
});

loadColumnsBtn.addEventListener("click", async () => {
  statusText.textContent = "Fetching Columns...";
  let source = sourceSelect.value;

  const body = {
    source,
    ...(source === "clickhouse" ? {
      host: document.getElementById("host").value,
      port: document.getElementById("port").value,
      database: document.getElementById("database").value,
      user: document.getElementById("user").value,
      jwt: document.getElementById("jwt").value
    } : {
      delimiter: document.getElementById("delimiter").value
    })
  };

  const fileInput = document.getElementById("flatfile");
  const formData = new FormData();
  formData.append("config", new Blob([JSON.stringify(body)], { type: "application/json" }));

  if (source === "flatfile") {
    formData.append("file", fileInput.files[0]);
  }

  const response = await fetch("/load-columns", {
    method: "POST",
    body: source === "flatfile" ? formData : JSON.stringify(body),
    headers: source === "clickhouse" ? { "Content-Type": "application/json" } : undefined,
  });

  const result = await response.json();
  columnsContainer.innerHTML = "";
  result.columns.forEach(col => {
    const label = document.createElement("label");
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = col;
    label.appendChild(checkbox);
    label.appendChild(document.createTextNode(col));
    columnsContainer.appendChild(label);
  });

  columnsSection.style.display = "block";
  statusText.textContent = "Columns Loaded.";
});

startBtn.addEventListener("click", async () => {
  statusText.textContent = "Starting ingestion...";
  selectedColumns = Array.from(columnsContainer.querySelectorAll("input:checked")).map(cb => cb.value);

  const source = sourceSelect.value;
  const config = {
    source,
    columns: selectedColumns,
    ...(source === "clickhouse" ? {
      host: document.getElementById("host").value,
      port: document.getElementById("port").value,
      database: document.getElementById("database").value,
      user: document.getElementById("user").value,
      jwt: document.getElementById("jwt").value
    } : {
      delimiter: document.getElementById("delimiter").value
    })
  };

  const fileInput = document.getElementById("flatfile");
  const formData = new FormData();
  formData.append("config", new Blob([JSON.stringify(config)], { type: "application/json" }));

  if (source === "flatfile") {
    formData.append("file", fileInput.files[0]);
  }

  const response = await fetch("/start-ingestion", {
    method: "POST",
    body: source === "flatfile" ? formData : JSON.stringify(config),
    headers: source === "clickhouse" ? { "Content-Type": "application/json" } : undefined,
  });

  const result = await response.json();
  statusText.textContent = "Completed.";
  resultText.textContent = `Records Ingested: ${result.count}`;
});
