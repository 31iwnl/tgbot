local htmlEntities = {}

local entities = {
  ["&"] = "&amp;",
  ["<"] = "&lt;",
  [">"] = "&gt;",
  ['"'] = "&quot;",
  ["'"] = "&#39;"
}

local reverse_entities = {}
for k, v in pairs(entities) do
  reverse_entities[v] = k
end

function htmlEntities.encode(str)
  return (str:gsub("[&<>\'\"]", entities))
end

function htmlEntities.decode(str)
  return (str:gsub("(&amp;|&lt;|&gt;|&quot;|&#39;)", reverse_entities))
end

return htmlEntities
